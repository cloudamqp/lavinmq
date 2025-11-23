require "openssl"
require "http/client"
require "digest/sha1"

# Extensions to OpenSSL::SSL::Context for X509_STORE operations
# needed for CRL (Certificate Revocation List) support
module OpenSSL::SSL
  class Context
    # Maximum CRL size to prevent memory exhaustion (10MB should be more than enough)
    MAX_CRL_SIZE = 10 * 1024 * 1024
    # Refresh CRL this long before it expires to avoid last-minute failures
    CRL_REFRESH_BUFFER = 1.hour

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
          # Get number of revoked certificates in the CRL
          revoked_count = count_revoked_certificates(crl)
          Log.info { "Loading CRL with #{revoked_count} revoked certificate(s)" }

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

    # Count the number of revoked certificates in a CRL
    private def count_revoked_certificates(crl : LibCrypto::X509CRL) : Int32
      revoked = LibCrypto.x509_crl_get_revoked(crl)
      return 0 if revoked.null?

      LibCrypto.sk_x509_revoked_num(revoked)
    end

    # Fetches CRL from HTTP/HTTPS URL with timeout, caching, and fallback to cached version
    # cache_dir: Optional directory to cache CRL files for offline fallback
    private def fetch_crl_from_url(url : String, cache_dir : String? = nil) : String
      url_hash = get_cache_hash(url) if cache_dir

      # Check if we have a valid cached CRL (timestamp in filename is not expired)
      if cache_dir && url_hash
        if cached = find_latest_valid_cache(cache_dir, url_hash)
          cache_path, expiry = cached
          Log.debug { "Using valid cached CRL from #{cache_path} (expires: #{expiry})" }
          return File.read(cache_path)
        else
          Log.info { "No valid cached CRL found, fetching fresh CRL from #{url}" }
        end
      end

      uri = URI.parse(url)

      # Try to fetch from URL
      begin
        crl_data = fetch_crl_from_remote(uri)

        # Save to cache if successful
        if cache_dir && url_hash && crl_data
          save_to_cache(cache_dir, url_hash, crl_data)
        end

        crl_data
      rescue ex : IO::TimeoutError | Socket::Error | IO::Error | OpenSSL::Error
        fallback_to_cached_crl(url, cache_dir, url_hash, ex)
      end
    end

    # Fetch CRL from remote HTTP/HTTPS server
    private def fetch_crl_from_remote(uri : URI) : String
      OpenSSL::X509.fetch_crl_from_url(uri)
    end

    # Fallback to using any cached CRL (even expired) when remote fetch fails
    private def fallback_to_cached_crl(url : String, cache_dir : String?, url_hash : String?, ex : Exception) : String
      return raise OpenSSL::Error.new("CRL fetch failed and no cache available: #{ex.message}") unless cache_dir && url_hash

      cache_base = File.join(cache_dir, "crl_cache")
      return raise OpenSSL::Error.new("CRL fetch failed and no cache available: #{ex.message}") unless Dir.exists?(cache_base)

      # Find ANY cached file for this URL, even if expired (sorted by timestamp)
      latest_file = Dir.glob(File.join(cache_base, "#{url_hash}.*.pem")).sort.last?

      return raise OpenSSL::Error.new("CRL fetch failed and no cache available: #{ex.message}") unless latest_file

      Log.warn { "CRL fetch from #{url} failed (#{ex.message}), using cached version (possibly expired): #{latest_file}" }
      File.read(latest_file)
    end

    # Generate cache base path from URL (without timestamp)
    # Returns the SHA1 hash that will be used as the base filename
    private def get_cache_hash(url : String) : String
      Digest::SHA1.hexdigest(url)
    end

    # Format timestamp for use in filename (unix timestamp)
    private def format_timestamp(time : Time) : String
      time.to_unix.to_s
    end

    # Parse timestamp from filename
    # Returns Time if successful, nil if filename format is invalid
    private def parse_timestamp_from_filename(filename : String) : Time?
      if match = filename.match(/\.(\d+)\.pem$/)
        Time.unix(match[1].to_i64)
      end
    rescue
      nil
    end

    # Find latest valid cached CRL for a given URL hash
    # Returns {path, expiry_time} if found, nil otherwise
    private def find_latest_valid_cache(cache_dir : String, url_hash : String) : Tuple(String, Time)?
      cache_base = File.join(cache_dir, "crl_cache")
      return unless Dir.exists?(cache_base)

      now = Time.utc
      # Sort by timestamp in filename and take the last (latest) valid one
      Dir.glob(File.join(cache_base, "#{url_hash}.*.pem")).sort.reverse_each do |file|
        if expiry = parse_timestamp_from_filename(file)
          return {file, expiry} if now < expiry
        end
      end

      nil
    end

    # Cleanup old CRLs for a given URL hash, keeping only the latest one
    private def cleanup_old_crls(cache_dir : String, url_hash : String, keep_file : String)
      cache_base = File.join(cache_dir, "crl_cache")
      return unless Dir.exists?(cache_base)

      Dir.glob(File.join(cache_base, "#{url_hash}.*.pem")).each do |file|
        next if file == keep_file
        File.delete(file)
        Log.debug { "Deleted old CRL cache: #{file}" }
      end
    rescue ex
      Log.debug { "Failed to cleanup old CRLs: #{ex.message}" }
    end

    # Save CRL data to cache file with timestamp in filename
    private def save_to_cache(cache_dir : String, url_hash : String, crl_data : String)
      # Extract nextUpdate timestamp from CRL
      next_update = extract_crl_next_update(crl_data)
      unless next_update
        Log.warn { "Failed to extract nextUpdate from CRL, skipping cache" }
        return
      end

      # Generate filename with timestamp
      cache_base = File.join(cache_dir, "crl_cache")
      Dir.mkdir_p(cache_base)

      timestamp_str = format_timestamp(next_update)
      cache_path = File.join(cache_base, "#{url_hash}.#{timestamp_str}.pem")

      # Write CRL to cache
      File.write(cache_path, crl_data)
      Log.debug { "Cached CRL to #{cache_path} (expires: #{next_update})" }

      # Cleanup old CRLs for this URL (keep only the latest)
      cleanup_old_crls(cache_dir, url_hash, cache_path)
    rescue ex
      Log.warn { "Failed to cache CRL: #{ex.message}" }
    end

    # Extract nextUpdate timestamp from CRL (for caching)
    private def extract_crl_next_update(crl_pem : String) : Time?
      bio = LibCrypto.bio_new_mem_buf(crl_pem, crl_pem.bytesize)
      return if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        return if crl.null?

        begin
          next_update = LibCrypto.x509_crl_get_next_update(crl)
          return if next_update.null?

          # Convert ASN1_TIME to unix timestamp using ASN1_TIME_diff
          current_time = LibCrypto.asn1_time_set(nil, Time.utc.to_unix)
          return if current_time.null?

          begin
            days = 0
            seconds = 0
            # Calculate difference: next_update - current_time
            result = LibCrypto.asn1_time_diff(pointerof(days), pointerof(seconds), current_time, next_update)
            return if result == 0 # Comparison failed

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

  # Fetch CRL from URL and cache it without loading into a TLS context
  # This is used for background CRL updates
  def self.fetch_and_cache_crl(url : String, cache_dir : String)
    uri = URI.parse(url)
    url_hash = Digest::SHA1.hexdigest(url)

    # Fetch CRL from remote server using the shared fetch logic
    crl_data = fetch_crl_from_url(uri)

    # Parse and extract expiration timestamp
    expiry = extract_crl_next_update(crl_data)
    unless expiry
      raise OpenSSL::Error.new("Failed to extract CRL expiration timestamp")
    end

    # Save to cache with timestamp in filename
    cache_base = File.join(cache_dir, "crl_cache")
    Dir.mkdir_p(cache_base)

    timestamp_str = expiry.to_unix.to_s
    cache_path = File.join(cache_base, "#{url_hash}.#{timestamp_str}.pem")

    File.write(cache_path, crl_data)

    # Clean up old CRL files for this URL
    cleanup_old_crls(cache_base, url_hash, timestamp_str)
  end

  # Fetch CRL from remote HTTP/HTTPS server
  # This is a shared utility method used by both Context and X509 module
  protected def self.fetch_crl_from_url(uri : URI) : String
    HTTP::Client.new(uri) do |client|
      client.connect_timeout = 10.seconds
      client.read_timeout = 10.seconds

      headers = HTTP::Headers{
        "User-Agent" => "LavinMQ/#{LavinMQ::VERSION}",
      }

      response = client.get(uri.request_target, headers: headers)
      unless response.success?
        raise OpenSSL::Error.new("CRL fetch failed: HTTP #{response.status_code}")
      end

      # Check Content-Length header if present
      if content_length = response.headers["Content-Length"]?
        size = content_length.to_i64
        if size > OpenSSL::SSL::Context::MAX_CRL_SIZE
          raise OpenSSL::Error.new("CRL too large: #{size} bytes (max: #{OpenSSL::SSL::Context::MAX_CRL_SIZE})")
        end
      end

      body = response.body
      if body.bytesize > OpenSSL::SSL::Context::MAX_CRL_SIZE
        raise OpenSSL::Error.new("CRL too large: #{body.bytesize} bytes (max: #{OpenSSL::SSL::Context::MAX_CRL_SIZE})")
      end

      body
    end
  end

  # Extract nextUpdate timestamp from CRL PEM string
  private def self.extract_crl_next_update(crl_pem : String) : Time?
    bio = LibCrypto.bio_new_mem_buf(crl_pem, crl_pem.bytesize)
    return if bio.null?

    begin
      crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
      return if crl.null?

      begin
        next_update = LibCrypto.x509_crl_get_next_update(crl)
        return if next_update.null?

        current_time = LibCrypto.asn1_time_new
        return if current_time.null?

        begin
          days = 0
          seconds = 0
          result = LibCrypto.asn1_time_diff(pointerof(days), pointerof(seconds), current_time, next_update)
          return if result == 0

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

  # Delete old CRL files for the same URL hash
  private def self.cleanup_old_crls(cache_base : String, url_hash : String, keep_timestamp : String)
    Dir.glob(File.join(cache_base, "#{url_hash}.*.pem")).each do |file|
      basename = File.basename(file, ".pem")
      file_timestamp = basename.split('.').last
      File.delete(file) if file_timestamp != keep_timestamp
    end
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
    return if count < 0 # Invalid count

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
  union GeneralNameData
    ptr : LibC::Char*
    otherName : Void*                      # ameba:disable Naming/VariableNames
    rfc822Name : ASN1String                # ameba:disable Naming/VariableNames
    dNSName : ASN1String                   # ameba:disable Naming/VariableNames
    x400Address : Void*                    # ameba:disable Naming/VariableNames
    directoryName : Void*                  # ameba:disable Naming/VariableNames
    ediPartyName : Void*                   # ameba:disable Naming/VariableNames
    uniformResourceIdentifier : ASN1String # ameba:disable Naming/VariableNames
    iPAddress : ASN1String                 # ameba:disable Naming/VariableNames
    registeredID : Void*                   # ameba:disable Naming/VariableNames
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

  # Get revoked certificates from CRL
  fun x509_crl_get_revoked = X509_CRL_get_REVOKED(crl : X509CRL) : Void*

  # Get count of revoked certificates in stack
  fun sk_x509_revoked_num = OPENSSL_sk_num(sk : Void*) : LibC::Int

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
    to : ASN1Time,
  ) : LibC::Int
end
