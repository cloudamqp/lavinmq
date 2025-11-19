require "./certificate_provider"
require "http/client"
require "http/headers"
require "json"

module LavinMQ
  module TrustStoreProvider
    class HttpProvider < CertificateProvider
      @headers : ::HTTP::Headers?
      @tls_context : ::OpenSSL::SSL::Context::Client?
      @last_modified : ::Time?
      @cached_certificates : Array(String)

      def initialize(@url : String, headers : ::HTTP::Headers? = nil, tls_context : ::OpenSSL::SSL::Context::Client? = nil)
        @uri = URI.parse(@url)
        @headers = headers
        @tls_context = tls_context
        @last_modified = nil
        @cached_certificates = Array(String).new
      end

      def name : String
        "http:#{@url}"
      end

      def load_certificates : Array(String)
        begin
          # Fetch certificate list
          cert_list = fetch_certificate_list
          # If 304 Not Modified, return cached certificates
          return @cached_certificates if cert_list.nil?

          # Download each certificate
          certificates = Array(String).new
          cert_list.each do |cert_info|
            cert_id = cert_info["id"].as_s
            cert_path = cert_info["path"].as_s

            if cert_pem = download_certificate(cert_path)
              certificates << cert_pem
              Log.debug { "Downloaded certificate #{cert_id} from #{cert_path}" }
            end
          end

          # Cache the new certificates
          @cached_certificates = certificates
          Log.info { "Loaded #{certificates.size} certificates from #{@url}" }
          certificates
        rescue ex : Socket::ConnectError | Socket::Error
          Log.warn { "Failed to connect to #{@url}: #{ex.message}, continuing with cached certificates" }
          @cached_certificates
        rescue ex : IO::Error
          Log.warn { "IO error fetching certificates from #{@url}: #{ex.message}, continuing with cached certificates" }
          @cached_certificates
        rescue ex : JSON::ParseException
          Log.warn { "Invalid JSON response from #{@url}: #{ex.message}, continuing with cached certificates" }
          @cached_certificates
        end
      end

      private def fetch_certificate_list : Array(JSON::Any)?
        client = create_http_client
        headers = @headers || ::HTTP::Headers.new

        # Add If-Modified-Since header if we have a last modified time
        if last_modified = @last_modified
          headers["If-Modified-Since"] = ::HTTP.format_time(last_modified)
        end

        path = @uri.path
        path = "/" if path.nil? || path.empty?
        response = client.get(path, headers: headers)

        case response.status_code
        when 304 # Not Modified
          Log.debug { "Certificate list not modified since #{@last_modified}" }
          return nil
        when 200
          # Update last modified time
          if last_modified_header = response.headers["Last-Modified"]?
            @last_modified = ::HTTP.parse_time(last_modified_header)
          end

          json = JSON.parse(response.body)
          json["certificates"].as_a
        else
          Log.error { "HTTP error fetching certificate list: #{response.status_code}" }
          nil
        end
      ensure
        client.try &.close
      end

      private def download_certificate(cert_path : String) : String?
        client = create_http_client
        headers = @headers || ::HTTP::Headers.new

        # Construct full path
        base_path = @uri.path
        base_path = "/" if base_path.nil? || base_path.empty?
        full_path = File.join(base_path, cert_path)

        response = client.get(full_path, headers: headers)

        if response.status_code == 200
          # Return raw PEM content, no parsing needed
          response.body
        else
          Log.warn { "Failed to download certificate from #{cert_path}: HTTP #{response.status_code}" }
          nil
        end
      ensure
        client.try &.close
      end

      private def create_http_client : ::HTTP::Client
        client = ::HTTP::Client.new(@uri.host.not_nil!, @uri.port || (@uri.scheme == "https" ? 443 : 80), tls: @tls_context)
        client.connect_timeout = 10.seconds
        client.read_timeout = 30.seconds
        client
      end
    end
  end
end
