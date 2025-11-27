require "openssl"
require "http/client"
require "digest/sha1"

# Extensions to OpenSSL::SSL::Context for X509_STORE operations
# needed for CRL (Certificate Revocation List) support
module OpenSSL::SSL
  class Context
    # Load CRL from a PEM file into the context's X509_STORE
    def add_crl(crl_path : String)
      store = OpenSSL::X509::Store.new(to_unsafe)
      store.load_crl_into_store(crl_path)
      store.enable_crl_checking
    end

    # Enables CRL fetching and loading from CDP URLs found in CA certificates
    # ca_cert_path: Path to PEM file with CA certificates
    # Returns a channel to control the background updater
    def set_ca_with_cdp(ca_cert_path : String, cache_dir : String) : Channel(Nil)
      # Iterate over all certificates in the CA bundle to extract CDP URLs
      crl_urls = OpenSSL::X509.extract_crl_urls_from_cert(ca_cert_path)
      Log.info { "Found CRL Distribution Points in CA certificates: #{crl_urls.join(", ")}" }

      fetch_and_load_crls(ca_cert_path, crl_urls, cache_dir)

      # Start background updater for periodic CRL updates
      updater_channel = start_crl_updater(ca_cert_path, crl_urls, cache_dir)

      # updater_channel can be used to trigger manual updates or stop the updater
      updater_channel
    end

    # Fetch CRL from URL and cache it without loading into a TLS context
    # This is used for background CRL updates
    # Returns the path to the cached CRL file
    # Falls back to cached CRL if fetch fails and cache exists
    # Raises if fetch fails and no cache is available
    private def fetch_and_cache_crl(url : String, cache_dir : String) : String
      uri = URI.parse(url)
      cache_base = File.join(cache_dir, "crl_cache")
      Dir.mkdir_p(cache_base)

      url_hash = Digest::SHA1.hexdigest(url)
      cache_path = File.join(cache_base, "#{url_hash}.crl")
      etag_path = File.join(cache_base, "#{url_hash}.etag")

      begin
        HTTP::Client.new(uri) do |client|
          client.connect_timeout = 5.seconds
          client.read_timeout = 5.seconds
          headers = HTTP::Headers{"User-Agent" => "LavinMQ/#{LavinMQ::VERSION}"}

          # Add conditional request header if we have an ETag
          headers["If-None-Match"] = File.read(etag_path) if File.exists?(etag_path)

          client.get(uri.request_target, headers: headers) do |response|
            case response.status_code
            when 304
              Log.debug { "CRL not modified (HTTP 304): #{url}" }
            when 200
              # Write response body to cache file
              File.open(cache_path, "w") do |file|
                IO.copy(response.body_io, file)
              end
              # Save ETag for future requests
              if new_etag = response.headers["ETag"]?
                File.write(etag_path, new_etag)
              else
                File.delete?(etag_path)
              end
            else
              raise OpenSSL::Error.new("CRL fetch failed: HTTP #{response.status_code}")
            end
          end
        end
      rescue ex : Socket::Error | IO::TimeoutError
        # If fetch fails, try to use cached CRL
        if File.exists?(cache_path)
          Log.warn(exception: ex) { "CRL fetch failed for #{url}: #{ex.message}, using cached CRL" }
        else
          raise ex
        end
      end
      cache_path
    end

    private def earliest_next_update(crl_paths : Array(String)) : Time?
      earliest : Time? = nil
      crl_paths.each do |crl_path|
        next_update = OpenSSL::X509.get_crl_next_update(crl_path)
        if next_update && (earliest.nil? || next_update < earliest)
          earliest = next_update
        end
      end
      earliest
    end

    # Start background CRL updater - single fiber that updates all CRLs
    # Uses the earliest nextUpdate timestamp to determine when to check next
    # fallback_interval: Interval to use if no nextUpdate can be read (default: 1 hour)
    private def start_crl_updater(ca_cert_path, crl_urls, cache_dir, fallback_interval : Time::Span = 1.hour)
      Log.info { "Starting background CRL updater for #{crl_urls.size} CDP URL(s)" }
      updater_channel = Channel(Nil).new
      spawn(name: "CRL Updater") { crl_updater(updater_channel, ca_cert_path, crl_urls, cache_dir, fallback_interval) }
      updater_channel
    end

    private def crl_updater(channel : Channel(Nil), ca_cert_path : String, crl_urls : Array(String), cache_dir : String, fallback_interval : Time::Span)
      return if crl_urls.empty?

      loop do
        # Get cache file path for a given CDP URL
        crl_paths = crl_urls.map do |url|
          File.join(cache_dir, "crl_cache", "#{Digest::SHA1.hexdigest(url)}.crl")
        end

        # Find the earliest nextUpdate timestamp across all CRLs
        sleep_duration =
          if earliest_next_update = earliest_next_update(crl_paths)
            # Sleep until earliest nextUpdate, or fallback if it's in the past
            duration = earliest_next_update - Time.utc
            if duration > 0.seconds
              Log.info { "Next CRL update scheduled at #{earliest_next_update} (in #{duration.total_hours.round(1)} hours)" }
              duration
            else
              Log.warn { "Earliest CRL nextUpdate is in the past, using fallback interval" }
              fallback_interval
            end
          else
            Log.debug { "Could not read nextUpdate from any CRL, using fallback interval" }
            fallback_interval
          end

        # Wait for either the scheduled duration or stop signal
        select
        when channel.receive
          Log.debug { "Manual CRL update triggered" }
        when timeout(sleep_duration)
          Log.debug { "Time triggered CRL update" }
        end

        # Fetch and load updated CRLs
        fetch_and_load_crls(ca_cert_path, crl_urls, cache_dir)
      rescue Channel::ClosedError
        Log.debug { "CRL updater closed, shutting down" }
        break
      rescue ex
        Log.error(exception: ex) { "Error in CRL updater: #{ex.message}" }
        Log.error { "Retrying CRL update in 1 minute" }
        sleep 1.minute
      end
    end

    # Fetch and load CRLs from tracked CDP URLs
    # Creates a new X509_STORE with CA certs and CRLs, then replaces the context's store
    private def fetch_and_load_crls(ca_cert_path : String, crl_urls : Array(String), cache_dir : String)
      # Fetch and cache CRLs, returning their paths
      crl_paths = crl_urls.map { |url| fetch_and_cache_crl(url, cache_dir) }

      # Create a new X509_STORE to replace the existing one
      # This ensures old CRLs are removed when updating
      new_store = OpenSSL::X509::Store.new

      # Load CA certificates into the new store
      new_store.load_ca_certs_into_store(ca_cert_path)

      # Load each CRL into the new store
      crl_paths.each do |crl_path|
        new_store.load_crl_into_store(crl_path)
      end

      # Only enable CRL checking if we have loaded any CRLs
      # otherwise verification will fail
      new_store.enable_crl_checking unless crl_paths.empty?

      # Replace the context's store (this frees the old store)
      new_store.assign(to_unsafe)
    end

    # Configure minimum TLS version based on version string
    # Valid values: "1.0", "1.1", "1.2", "1.3", "" (default: 1.2)
    def min_version=(version : String)
      case version
      when "1.0"
        remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                       OpenSSL::SSL::Options::NO_TLS_V1_1 |
                       OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.1"
        remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        add_options(OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.2", ""
        remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.3"
        add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      else
        Log.warn { "Unrecognized TLS min version: '#{version}'" }
      end
    end
  end
end

# Helper module for CRL operations
module OpenSSL::X509
  class Store
    @store : LibCrypto::X509_STORE
    @assigned : Bool

    def initialize(ctx : LibSSL::SSLContext)
      @store = LibSSL.ssl_ctx_get_cert_store(ctx)
      raise OpenSSL::Error.new("SSL_CTX_get_cert_store") if @store.null?
      @assigned = true
    end

    def initialize
      @store = LibCrypto.x509_store_new
      raise OpenSSL::Error.new("X509_STORE_new") if @store.null?
      @assigned = false
    end

    def to_unsafe : LibCrypto::X509_STORE
      @store
    end

    def assign(ctx : LibSSL::SSLContext) : Nil
      LibSSL.ssl_ctx_set_cert_store(ctx, @store)
      @assigned = true
    end

    def finalize
      LibCrypto.x509_store_free(@store) unless @assigned
    end

    def enable_crl_checking
      flags = LibCrypto::X509VerifyFlags::CRL_CHECK |
              LibCrypto::X509VerifyFlags::CRL_CHECK_ALL |
              LibCrypto::X509VerifyFlags::EXTENDED_CRL_SUPPORT |
              LibCrypto::X509VerifyFlags::TRUSTED_FIRST
      ret = LibCrypto.x509_store_set_flags(@store, flags)
      raise OpenSSL::Error.new("X509_STORE_set_flags") unless ret == 1

      Log.info { "Enabled CRL checking on X509_STORE" }
    end

    # Load CA certificates from a PEM file into an X509_STORE
    def load_ca_certs_into_store(ca_cert_path : String)
      bio = LibCrypto.bio_new_file(ca_cert_path, "r")
      raise OpenSSL::Error.new("BIO_new_file: #{ca_cert_path}") if bio.null?

      begin
        cert_count = 0
        loop do
          cert = LibCrypto.pem_read_bio_x509(bio, nil, nil, nil)
          break if cert.null? # No more certificates

          begin
            ret = LibCrypto.x509_store_add_cert(@store, cert)
            raise OpenSSL::Error.new("X509_STORE_add_cert") unless ret == 1
            cert_count += 1
          ensure
            LibCrypto.x509_free(cert)
          end
        end

        Log.debug { "Loaded #{cert_count} CA certificate(s) from #{ca_cert_path}" }
      ensure
        LibCrypto.bio_free(bio)
      end
    end

    # Load a CRL from a PEM file into an X509_STORE
    # enable_flags: If true, enables CRL checking flags on the store
    def load_crl_into_store(crl_path : String)
      bio = LibCrypto.bio_new_file(crl_path, "r")
      raise OpenSSL::Error.new("BIO_new_file: #{crl_path}") if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        raise OpenSSL::Error.new("PEM_read_bio_X509_CRL - CRL may be invalid: #{crl_path}") if crl.null?

        begin
          revoked_count = count_revoked_certificates(crl)
          Log.info { "Loading CRL with #{revoked_count} revoked certificate(s) from #{crl_path}" }

          ret = LibCrypto.x509_store_add_crl(@store, crl)
          raise OpenSSL::Error.new("X509_STORE_add_crl") unless ret == 1
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

      LibCrypto.openssl_sk_num(revoked)
    end
  end

  # Extract CRL Distribution Point URLs from all certificates in a PEM bundle
  # This version provides detailed logging similar to the C implementation
  # Returns array of CDP URIs found (may contain duplicates if multiple certs have same CDP)
  # Raises OpenSSL::Error if the file cannot be opened
  def self.extract_crl_urls_from_cert(ca_file_path : String) : Array(String)
    uris = [] of String
    cert_count = 0

    bio = LibCrypto.bio_new_file(ca_file_path, "r")
    raise OpenSSL::Error.new("Could not open CA file: #{ca_file_path}") if bio.null?

    Log.info { "Starting to read certificates from bundle: #{ca_file_path}" }

    begin
      # Read certificates one by one until PEM_read_bio_X509 returns NULL
      loop do
        cert = LibCrypto.pem_read_bio_x509(bio, nil, nil, nil)
        break if cert.null?

        cert_count += 1
        Log.debug { "--- Processing Certificate ##{cert_count} ---" }

        begin
          # Find the CRL Distribution Points extension (NID_crl_distribution_points = 103)
          cdp = LibCrypto.x509_get_ext_d2i(cert, 103, nil, nil)
          if cdp.null?
            Log.debug { "No CDP extension found in Certificate ##{cert_count}" }
            next
          end

          begin
            # Iterate through all distribution points
            count = LibCrypto.openssl_sk_num(cdp)
            count.times do |j|
              dp = LibCrypto.openssl_sk_value(cdp, j).as(LibCrypto::DistPoint)
              next if dp.null?

              # Extract URL from distribution point
              if url = extract_url_from_dist_point(dp)
                Log.debug { "  -> CDP URI found: #{url}" }
                uris << url
              end
            end
          ensure
            LibCrypto.crl_dist_points_free(cdp)
          end
        ensure
          LibCrypto.x509_free(cert)
        end
      end
    ensure
      LibCrypto.bio_free(bio)
    end

    Log.info { "Finished processing. Total certificates processed: #{cert_count}. Total CDP URIs found: #{uris.size}" }
    uris
  end

  # Get the nextUpdate time from a CRL file
  # Returns nil if the field cannot be read
  def self.get_crl_next_update(crl_path : String) : Time?
    return unless File.exists?(crl_path)

    bio = LibCrypto.bio_new_file(crl_path, "r")
    raise OpenSSL::Error.new("BIO_new_file: #{crl_path}") if bio.null?

    begin
      crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
      return if crl.null?

      begin
        next_update = LibCrypto.x509_crl_get_next_update(crl)
        return if next_update.null?

        # Convert ASN1_TIME to Crystal Time
        tm = LibC::Tm.new
        ret = LibCrypto.asn1_time_to_tm(next_update, pointerof(tm))
        return if ret == 0

        # Convert tm to Time
        Time.utc(
          year: tm.tm_year + 1900,
          month: tm.tm_mon + 1,
          day: tm.tm_mday,
          hour: tm.tm_hour,
          minute: tm.tm_min,
          second: tm.tm_sec
        )
      ensure
        LibCrypto.x509_crl_free(crl)
      end
    ensure
      LibCrypto.bio_free(bio)
    end
  rescue ex
    Log.debug(exception: ex) { "Failed to read nextUpdate from CRL #{crl_path}: #{ex.message}" }
    nil
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

    # Cast to the actual structure type
    dist_point = dp.as(LibCrypto::DistPointStruct*)
    return if dist_point.value.distpoint.null?

    dpn = dist_point.value.distpoint
    return if dpn.value.type != 0 # 0 = fullname (GENERAL_NAMES)

    general_names = dpn.value.name.fullname
    general_names.null? ? nil : general_names
  end

  # Find HTTP/HTTPS URL in GENERAL_NAMES
  private def self.find_http_url_in_general_names(general_names : LibCrypto::GeneralNamesPtr) : String?
    count = LibCrypto.openssl_sk_num(general_names)
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
    gen_name = LibCrypto.openssl_sk_value(general_names, index)
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
  alias ASN1Time = Void*

  # DIST_POINT structure
  struct DistPointStruct
    distpoint : DistPointNameStruct* # DIST_POINT_NAME
    reasons : Void*                  # ASN1_BIT_STRING (different from ASN1_STRING)
    crl_issuer : GeneralNamesPtr     # GENERAL_NAMES
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

  # Create a BIO from file
  fun bio_new_file = BIO_new_file(filename : LibC::Char*, mode : LibC::Char*) : Bio*

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

  # Generic OpenSSL stack functions (work with any stack type via Void*)
  # Get number of elements in an OpenSSL stack
  fun openssl_sk_num = OPENSSL_sk_num(sk : Void*) : LibC::Int

  # Get element from OpenSSL stack by index
  fun openssl_sk_value = OPENSSL_sk_value(sk : Void*, idx : LibC::Int) : Void*

  # Free CRL Distribution Points
  fun crl_dist_points_free = CRL_DIST_POINTS_free(a : DistPoints)

  # Add a CRL to the X509_STORE
  fun x509_store_add_crl = X509_STORE_add_crl(ctx : X509_STORE, x : X509CRL) : LibC::Int

  # Free a CRL
  fun x509_crl_free = X509_CRL_free(a : X509CRL)

  # Get revoked certificates from CRL
  fun x509_crl_get_revoked = X509_CRL_get_REVOKED(crl : X509CRL) : Void*

  # Set X509 verification flags on the store
  fun x509_store_set_flags = X509_STORE_set_flags(ctx : X509_STORE, flags : X509VerifyFlags) : LibC::Int

  # Create a new X509_STORE
  fun x509_store_new = X509_STORE_new : X509_STORE

  # Free an X509_STORE (decrements reference count)
  fun x509_store_free = X509_STORE_free(store : X509_STORE)

  # Convert ASN1_STRING to UTF8
  fun asn1_string_to_utf8 = ASN1_STRING_to_UTF8(out : LibC::Char**, in : ASN1String) : LibC::Int

  # Free memory allocated by OpenSSL (OPENSSL_free is a macro for CRYPTO_free)
  fun openssl_free = CRYPTO_free(addr : Void*, file : LibC::Char*, line : LibC::Int)

  # Get the nextUpdate time from a CRL
  fun x509_crl_get_next_update = X509_CRL_get_nextUpdate(crl : X509CRL) : ASN1Time

  # Convert ASN1_TIME to time_t (Unix timestamp)
  fun asn1_time_to_tm = ASN1_TIME_to_tm(s : ASN1Time, tm : LibC::Tm*) : LibC::Int
end

# Additional LibSSL bindings for certificate verification
lib LibSSL
  # Get the result of certificate verification
  # Returns 0 (X509_V_OK) if verification succeeded
  fun ssl_get_verify_result = SSL_get_verify_result(ssl : LibSSL::SSL) : LibC::Long

  # Replace the X509_STORE in an SSL_CTX
  # The SSL_CTX takes ownership of the store and will free the old store
  fun ssl_ctx_set_cert_store = SSL_CTX_set_cert_store(ctx : SSLContext, store : LibCrypto::X509_STORE)
end

# Extension to OpenSSL::SSL::Socket to check verification result
class OpenSSL::SSL::Socket
  # Returns the verification result for the peer certificate
  # 0 (X509_V_OK) means verification succeeded
  def verify_result : LibC::Long
    LibSSL.ssl_get_verify_result(@ssl)
  end

  # Returns a human-readable explanation of the verification error
  # ameba:disable Metrics/CyclomaticComplexity
  def verify_error_string(code : LibC::Long) : String
    case code
    when  0 then "ok"
    when  2 then "unable to get issuer certificate"
    when  3 then "unable to get CRL"
    when  7 then "certificate signature failure"
    when  8 then "CRL signature failure"
    when  9 then "certificate is not yet valid"
    when 10 then "certificate has expired"
    when 11 then "CRL is not yet valid"
    when 12 then "CRL has expired"
    when 18 then "self signed certificate"
    when 19 then "self signed certificate in chain"
    when 20 then "unable to get local issuer certificate"
    when 21 then "unable to verify leaf signature"
    when 23 then "certificate revoked"
    when 26 then "invalid purpose"
    when 27 then "certificate untrusted"
    when 28 then "certificate rejected"
    else         "verification error #{code}"
    end
  end
end
