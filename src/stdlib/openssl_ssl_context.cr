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
