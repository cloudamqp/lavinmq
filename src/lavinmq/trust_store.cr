require "http/headers"
require "./trust_store/*"

module LavinMQ
  class TrustStore
    Log = LavinMQ::Log.for("trust_store")

    getter certificate_file : String
    property on_reload : Proc(Nil)?

    def initialize(@config : Config)
      @providers = Array(TrustStoreProvider::CertificateProvider).new
      @closed = false
      @mutex = Mutex.new
      @cert_count = 0
      @on_reload = nil

      # Create a temporary file for storing concatenated certificates
      @tempfile = File.tempfile("lavinmq-trust-store", ".pem")
      @certificate_file = @tempfile.path

      setup_providers
      load_certificates
      spawn refresh_loop, name: "TrustStore#refresh_loop" if @config.tls_trust_store_refresh_interval > 0
    end

    def close
      @closed = true
      @tempfile.delete if File.exists?(@tempfile.path)
    end

    # Reload certificates from all providers
    def reload
      Log.info { "Reloading trust store certificates" }
      load_certificates
    end

    # Get count of trusted certificates
    def size : Int32
      @mutex.synchronize { @cert_count }
    end

    private def setup_providers
      # Enable filesystem provider if directory is configured
      unless @config.tls_trust_store_directory.empty?
        @providers << TrustStoreProvider::FilesystemProvider.new(@config.tls_trust_store_directory)
        Log.info { "Enabled filesystem provider: #{@config.tls_trust_store_directory}" }
      end

      # Enable HTTP provider if URL is configured
      unless @config.tls_trust_store_url.empty?
        # Create TLS context for HTTP provider if needed
        tls_ctx = if @config.tls_trust_store_url.starts_with?("https")
                    ctx = OpenSSL::SSL::Context::Client.new
                    # Configure client cert if specified
                    unless @config.tls_trust_store_http_cert.empty?
                      ctx.certificate_chain = @config.tls_trust_store_http_cert
                      ctx.private_key = @config.tls_trust_store_http_key.empty? ? @config.tls_trust_store_http_cert : @config.tls_trust_store_http_key
                    end
                    # Configure CA for server verification if specified
                    ctx.ca_certificates = @config.tls_trust_store_http_ca unless @config.tls_trust_store_http_ca.empty?
                    ctx
                  end

        # Parse custom headers
        headers = ::HTTP::Headers.new
        @config.tls_trust_store_http_headers.each do |key, value|
          headers[key] = value
        end

        @providers << TrustStoreProvider::HttpProvider.new(@config.tls_trust_store_url, headers.empty? ? nil : headers, tls_ctx)
        Log.info { "Enabled HTTP provider: #{@config.tls_trust_store_url}" }
      end

      if @providers.empty?
        Log.warn { "No trust store providers configured" }
      end
    end

    private def load_certificates
      pem_content = String::Builder.new
      cert_count = 0

      # Include the configured local CA certificate first (if specified)
      unless @config.tls_ca_cert_path.empty?
        begin
          ca_cert = File.read(@config.tls_ca_cert_path)
          pem_content << ca_cert
          pem_content << "\n" unless ca_cert.ends_with?("\n")
          cert_count += 1
          Log.debug { "Included local CA certificate: #{@config.tls_ca_cert_path}" }
        rescue ex : File::NotFoundError
          Log.warn { "Configured CA certificate not found: #{@config.tls_ca_cert_path}" }
        rescue ex : IO::Error
          Log.warn { "Error reading CA certificate #{@config.tls_ca_cert_path}: #{ex.message}" }
        end
      end

      # Load certificates from all providers (filesystem, HTTP, etc.)
      @providers.each do |provider|
        begin
          certs = provider.load_certificates
          certs.each do |cert_pem|
            pem_content << cert_pem
            pem_content << "\n" unless cert_pem.ends_with?("\n")
            cert_count += 1
          end
        rescue ex
          Log.warn(exception: ex) { "Error loading certificates from #{provider.name}, continuing with other providers" }
        end
      end

      @mutex.synchronize do
        # Write all certificates to the temp file
        File.write(@tempfile.path, pem_content.to_s)
        @cert_count = cert_count
        Log.info { "Trust store now contains #{@cert_count} certificates in #{@certificate_file}" }
      end
    end

    private def refresh_loop
      loop do
        sleep @config.tls_trust_store_refresh_interval.seconds
        return if @closed
        begin
          reload
          # Trigger TLS context reload callback if set
          @on_reload.try &.call
        rescue ex
          Log.error(exception: ex) { "Error in trust store refresh loop" }
        end
      end
    end
  end
end
