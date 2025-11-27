module OpenSSL::X509
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
