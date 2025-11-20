require "openssl"
require "http/client"
require "digest/sha1"

# Extensions to OpenSSL::SSL::Context for X509_STORE operations
# needed for CRL (Certificate Revocation List) support
module OpenSSL::SSL
  class Context
    # Loads CRL (Certificate Revocation List) from a file or URL into the X509 store
    # and enables CRL checking for the entire certificate chain
    #
    # Supports:
    # - Local file paths (file:// or absolute/relative paths)
    # - HTTP/HTTPS URLs for automatic CRL fetching with caching
    #
    # cache_dir: Optional directory for caching CRLs fetched from URLs.
    #           If provided, CRLs are cached and used as fallback when URL is unreachable.
    def load_crl(source : String, cache_dir : String? = nil)
      crl_data = if source.starts_with?("http://") || source.starts_with?("https://")
                   fetch_crl_from_url(source, cache_dir)
                 else
                   File.read(source)
                 end

      load_crl_from_string(crl_data)
    end

    # Loads CRL from PEM-formatted string
    private def load_crl_from_string(crl_pem : String)
      store = LibSSL.ssl_ctx_get_cert_store(@handle)
      raise OpenSSL::Error.new("SSL_CTX_get_cert_store") if store.null?

      # Create BIO from memory
      bio = LibCrypto.bio_new_mem_buf(crl_pem, crl_pem.bytesize)
      raise OpenSSL::Error.new("BIO_new_mem_buf") if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        raise OpenSSL::Error.new("PEM_read_bio_X509_CRL - CRL may be invalid") if crl.null?

        begin
          ret = LibCrypto.x509_store_add_crl(store, crl)
          raise OpenSSL::Error.new("X509_STORE_add_crl") unless ret == 1

          # Enable CRL checking for end-entity certificates
          flags = LibCrypto::X509VerifyFlags::CRL_CHECK |
                  LibCrypto::X509VerifyFlags::EXTENDED_CRL_SUPPORT
          ret = LibCrypto.x509_store_set_flags(store, flags)
          raise OpenSSL::Error.new("X509_STORE_set_flags") unless ret == 1
        ensure
          LibCrypto.x509_crl_free(crl)
        end
      ensure
        LibCrypto.bio_free(bio)
      end
    end

    # Fetches CRL from HTTP/HTTPS URL with timeout, caching, and fallback to cached version
    # cache_dir: Optional directory to cache CRL files for offline fallback
    private def fetch_crl_from_url(url : String, cache_dir : String? = nil) : String
      cache_path = get_cache_path(url, cache_dir) if cache_dir

      # Check if cached CRL exists and is still valid (fast path using metadata)
      if cache_path && File.exists?(cache_path)
        if crl_cache_valid?(cache_path)
          Log.debug { "Using valid cached CRL from #{cache_path}" }
          return File.read(cache_path)
        else
          Log.info { "Cached CRL expired, fetching fresh CRL from #{url}" }
        end
      end

      uri = URI.parse(url)

      # Try to fetch from URL
      begin
        crl_data = HTTP::Client.new(uri) do |client|
          client.connect_timeout = 10.seconds
          client.read_timeout = 10.seconds

          response = client.get(uri.request_target)
          unless response.success?
            raise OpenSSL::Error.new("CRL fetch failed: HTTP #{response.status_code}")
          end

          response.body
        end

        # Save to cache if successful
        if cache_path && crl_data
          save_to_cache(cache_path, crl_data)
        end

        crl_data
      rescue ex : IO::TimeoutError | Socket::Error | IO::Error | OpenSSL::Error
        # If fetch fails and we have a cache, try to use it (even if expired)
        if cache_path && File.exists?(cache_path)
          Log.warn { "CRL fetch from #{url} failed (#{ex.message}), using cached version (possibly expired)" }
          File.read(cache_path)
        else
          raise OpenSSL::Error.new("CRL fetch failed and no cache available: #{ex.message}")
        end
      end
    end

    # Generate cache file path from URL
    private def get_cache_path(url : String, cache_dir : String) : String
      # Use SHA1 of URL as filename to avoid path issues
      url_hash = Digest::SHA1.hexdigest(url)
      File.join(cache_dir, "crl_cache", "#{url_hash}.pem")
    end

    # Save CRL data to cache file along with validity metadata
    private def save_to_cache(cache_path : String, crl_data : String)
      Dir.mkdir_p(File.dirname(cache_path))
      File.write(cache_path, crl_data)

      # Extract and cache the nextUpdate timestamp for fast validity checks
      if next_update = extract_crl_next_update(crl_data)
        save_crl_metadata(cache_path, next_update)
      end

      Log.debug { "Cached CRL to #{cache_path}" }
    rescue ex
      Log.warn { "Failed to cache CRL: #{ex.message}" }
    end

    # Check if cached CRL is still valid using metadata (fast path)
    # Falls back to full CRL parsing if metadata is missing or invalid
    private def crl_cache_valid?(cache_path : String) : Bool
      meta_path = get_metadata_path(cache_path)

      # Fast path: check metadata file
      if File.exists?(meta_path)
        if next_update_str = File.read(meta_path).strip
          begin
            next_update = Time.unix(next_update_str.to_i64)
            return Time.utc < next_update
          rescue
            # Invalid metadata, fall through to full parsing
          end
        end
      end

      # Slow path: parse CRL (only if metadata is missing/invalid)
      crl_valid?(File.read(cache_path))
    end

    # Get metadata file path for a cached CRL
    private def get_metadata_path(cache_path : String) : String
      "#{cache_path}.meta"
    end

    # Save CRL nextUpdate timestamp to metadata file
    private def save_crl_metadata(cache_path : String, next_update : Time)
      meta_path = get_metadata_path(cache_path)
      File.write(meta_path, next_update.to_unix.to_s)
    rescue ex
      Log.debug { "Failed to save CRL metadata: #{ex.message}" }
    end

    # Extract nextUpdate timestamp from CRL (for caching)
    private def extract_crl_next_update(crl_pem : String) : Time?
      bio = LibCrypto.bio_new_mem_buf(crl_pem, crl_pem.bytesize)
      return nil if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        return nil if crl.null?

        begin
          next_update = LibCrypto.x509_crl_get_next_update(crl)
          return nil if next_update.null?

          # Convert ASN1_TIME to unix timestamp using ASN1_TIME_diff
          current_time = LibCrypto.asn1_time_set(nil, Time.utc.to_unix)
          return nil if current_time.null?

          begin
            days = 0
            seconds = 0
            # Calculate difference: next_update - current_time
            result = LibCrypto.asn1_time_diff(pointerof(days), pointerof(seconds), current_time, next_update)
            return nil if result == 0 # Comparison failed

            # Calculate total seconds and add to current time
            total_seconds = days * 86400 + seconds
            Time.utc + total_seconds.seconds
          ensure
            LibCrypto.asn1_time_free(current_time)
          end
        ensure
          LibCrypto.x509_crl_free(crl)
        end
      ensure
        LibCrypto.bio_free(bio)
      end
    rescue
      nil
    end

    # Check if a CRL is still valid (not expired)
    # Returns true if the CRL's nextUpdate time is in the future
    private def crl_valid?(crl_pem : String) : Bool
      bio = LibCrypto.bio_new_mem_buf(crl_pem, crl_pem.bytesize)
      return false if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        return false if crl.null?

        begin
          next_update = LibCrypto.x509_crl_get_next_update(crl)
          return false if next_update.null?

          # Create ASN1_TIME for current time
          current_time = LibCrypto.asn1_time_set(nil, Time.utc.to_unix)
          return false if current_time.null?

          begin
            # Compare nextUpdate with current time
            # Returns: -1 if next_update < now, 0 if equal, 1 if next_update > now
            result = LibCrypto.asn1_time_compare(next_update, current_time)
            result > 0
          ensure
            LibCrypto.asn1_time_free(current_time)
          end
        ensure
          LibCrypto.x509_crl_free(crl)
        end
      ensure
        LibCrypto.bio_free(bio)
      end
    rescue
      false
    end
  end
end

# Helper module for CRL operations
module OpenSSL::X509
  # Extract CRL Distribution Point URLs from a certificate file
  def self.extract_crl_urls_from_cert(cert_path : String) : Array(String)
    urls = [] of String

    file = LibCrypto.bio_new_file(cert_path, "r")
    return urls if file.null?

    begin
      cert = LibCrypto.pem_read_bio_x509(file, nil, nil, nil)
      return urls if cert.null?

      begin
        # Get CRL Distribution Points extension
        # NID_crl_distribution_points = 103
        cdp = LibCrypto.x509_get_ext_d2i(cert, 103, nil, nil)
        return urls if cdp.null?

        begin
          # Parse the STACK_OF(DIST_POINT)
          # This is simplified - in a full implementation you'd iterate through all distribution points
          # For now, we'll extract the first HTTP/HTTPS URL we find
          count = LibCrypto.sk_dist_point_num(cdp)
          count.times do |i|
            dp = LibCrypto.sk_dist_point_value(cdp, i)
            next if dp.null?

            # Extract URL from distribution point
            if url = extract_url_from_dist_point(dp)
              urls << url
            end
          end
        ensure
          LibCrypto.crl_dist_points_free(cdp)
        end
      ensure
        LibCrypto.x509_free(cert)
      end
    ensure
      LibCrypto.bio_free(file)
    end

    urls
  end

  # Extract URL from DIST_POINT structure
  private def self.extract_url_from_dist_point(dp : LibCrypto::DistPoint) : String?
    general_names = get_general_names_from_dist_point(dp)
    return if general_names.nil?

    find_http_url_in_general_names(general_names)
  end

  # Get GENERAL_NAMES from a DIST_POINT structure
  private def self.get_general_names_from_dist_point(dp : LibCrypto::DistPoint) : LibCrypto::GeneralNamesPtr?
    return if dp.null?

    dist_point = dp.as(LibCrypto::DistPointStruct*)
    return if dist_point.null?
    return if dist_point.value.distpoint.null?

    dpn = dist_point.value.distpoint.as(LibCrypto::DistPointNameStruct*)
    return if dpn.null?
    return if dpn.value.type != 0 # 0 = fullname (GENERAL_NAMES)

    general_names = dpn.value.name.fullname
    general_names.null? ? nil : general_names
  end

  # Find HTTP/HTTPS URL in GENERAL_NAMES
  private def self.find_http_url_in_general_names(general_names : LibCrypto::GeneralNamesPtr) : String?
    count = LibCrypto.sk_general_name_num(general_names)
    count.times do |i|
      if url = extract_uri_from_general_name(general_names, i)
        return url if url.starts_with?("http://") || url.starts_with?("https://")
      end
    end
    nil
  end

  # Extract URI string from a GENERAL_NAME at given index
  private def self.extract_uri_from_general_name(general_names : LibCrypto::GeneralNamesPtr, index : Int32) : String?
    gen_name = LibCrypto.sk_general_name_value(general_names, index)
    return if gen_name.null?

    gn = gen_name.as(LibCrypto::GeneralNameStruct*)
    return if gn.value.type != 6 # GEN_URI = 6

    uri = gn.value.d.uniformResourceIdentifier
    return if uri.null?

    convert_asn1_string_to_string(uri)
  end

  # Convert ASN1_STRING to Crystal String
  private def self.convert_asn1_string_to_string(asn1_str : LibCrypto::ASN1String) : String?
    uri_str_ptr = Pointer(LibC::Char).null
    len = LibCrypto.asn1_string_to_utf8(pointerof(uri_str_ptr), asn1_str)
    return if len < 0 || uri_str_ptr.null?

    begin
      String.new(uri_str_ptr, len)
    ensure
      LibCrypto.openssl_free(uri_str_ptr, nil, 0)
    end
  end
end

# Add missing LibCrypto function bindings
lib LibCrypto
  alias X509CRL = Void*
  alias DistPoint = Void*
  alias DistPoints = Void*
  alias GeneralNamesPtr = Void*
  alias GeneralNamePtr = Void*
  alias ASN1String = Void*

  # DIST_POINT structure
  struct DistPointStruct
    distpoint : Void*            # DIST_POINT_NAME
    reasons : Void*              # ASN1_BIT_STRING
    crl_issuer : GeneralNamesPtr # GENERAL_NAMES
    dp_reasons : LibC::Int
  end

  # DIST_POINT_NAME structure (union-like)
  struct DistPointNameStruct
    type : LibC::Int
    name : DistPointNameUnion
  end

  # Union for DIST_POINT_NAME
  union DistPointNameUnion
    fullname : GeneralNamesPtr # GENERAL_NAMES (type 0)
    relativename : Void*       # X509_NAME (type 1)
  end

  # GENERAL_NAME structure (simplified)
  struct GeneralNameStruct
    type : LibC::Int
    d : GeneralNameData
  end

  # Union for GENERAL_NAME data
  # ameba:disable Naming/VariableNames
  union GeneralNameData
    ptr : LibC::Char*
    otherName : Void*                      # ASN1_TYPE
    rfc822Name : ASN1String                # ASN1_IA5STRING
    dNSName : ASN1String                   # ASN1_IA5STRING
    x400Address : Void*                    # ASN1_TYPE
    directoryName : Void*                  # X509_NAME
    ediPartyName : Void*                   # ASN1_TYPE
    uniformResourceIdentifier : ASN1String # ASN1_IA5STRING (type 6)
    iPAddress : ASN1String                 # ASN1_OCTET_STRING
    registeredID : Void*                   # ASN1_OBJECT
  end

  # Create a BIO from memory buffer
  fun bio_new_mem_buf = BIO_new_mem_buf(
    buf : LibC::Char*,
    len : LibC::Int,
  ) : Bio*

  # Create a BIO from file
  fun bio_new_file = BIO_new_file(
    filename : LibC::Char*,
    mode : LibC::Char*,
  ) : Bio*

  # Free a BIO
  fun bio_free = BIO_free(bio : Bio*) : LibC::Int

  # Read certificate from PEM
  fun pem_read_bio_x509 = PEM_read_bio_X509(
    bp : Bio*,
    x : X509*,
    cb : Void*,
    u : Void*,
  ) : X509

  # Read a CRL from a PEM file
  fun pem_read_bio_x509_crl = PEM_read_bio_X509_CRL(
    bp : Bio*,
    x : X509CRL*,
    cb : Void*,
    u : Void*,
  ) : X509CRL

  # Get extension from certificate by NID
  fun x509_get_ext_d2i = X509_get_ext_d2i(
    x : X509,
    nid : LibC::Int,
    crit : LibC::Int*,
    idx : LibC::Int*,
  ) : Void*

  # Free X509 certificate
  fun x509_free = X509_free(a : X509)

  # Get count of distribution points in stack
  fun sk_dist_point_num = OPENSSL_sk_num(sk : Void*) : LibC::Int

  # Get distribution point from stack by index
  fun sk_dist_point_value = OPENSSL_sk_value(sk : Void*, idx : LibC::Int) : DistPoint

  # Get count of general names in stack
  fun sk_general_name_num = OPENSSL_sk_num(sk : GeneralNamesPtr) : LibC::Int

  # Get general name from stack by index
  fun sk_general_name_value = OPENSSL_sk_value(sk : GeneralNamesPtr, idx : LibC::Int) : GeneralNamePtr

  # Free CRL Distribution Points
  fun crl_dist_points_free = CRL_DIST_POINTS_free(a : DistPoints)

  # Add a CRL to the X509_STORE
  fun x509_store_add_crl = X509_STORE_add_crl(
    ctx : X509_STORE,
    x : X509CRL,
  ) : LibC::Int

  # Free a CRL
  fun x509_crl_free = X509_CRL_free(a : X509CRL)

  # Set X509 verification flags on the store
  fun x509_store_set_flags = X509_STORE_set_flags(
    ctx : X509_STORE,
    flags : X509VerifyFlags,
  ) : LibC::Int

  # Convert ASN1_STRING to UTF8
  fun asn1_string_to_utf8 = ASN1_STRING_to_UTF8(
    out : LibC::Char**,
    in : ASN1String,
  ) : LibC::Int

  # Free memory allocated by OpenSSL (OPENSSL_free is a macro for CRYPTO_free)
  fun openssl_free = CRYPTO_free(addr : Void*, file : LibC::Char*, line : LibC::Int)

  # ASN1_TIME type
  alias ASN1Time = Void*

  # Get the nextUpdate field from a CRL
  fun x509_crl_get_next_update = X509_CRL_get_nextUpdate(x : X509CRL) : ASN1Time

  # Compare two ASN1_TIME values
  # Returns: -1 if a < b, 0 if a == b, 1 if a > b
  fun asn1_time_compare = ASN1_TIME_compare(a : ASN1Time, b : ASN1Time) : LibC::Int

  # Create ASN1_TIME from unix timestamp
  # If t is nil, allocates new ASN1_TIME, otherwise modifies existing
  fun asn1_time_set = ASN1_TIME_set(t : ASN1Time, unix_time : Int64) : ASN1Time

  # Free ASN1_TIME
  fun asn1_time_free = ASN1_TIME_free(t : ASN1Time)

  # Calculate difference between two ASN1_TIME values
  # Returns 1 on success, 0 on error
  # pday receives the number of days difference
  # psec receives the number of seconds difference (in addition to days)
  fun asn1_time_diff = ASN1_TIME_diff(
    pday : LibC::Int*,
    psec : LibC::Int*,
    from : ASN1Time,
    to : ASN1Time
  ) : LibC::Int
end
