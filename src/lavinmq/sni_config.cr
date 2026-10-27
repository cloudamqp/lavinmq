require "openssl"

module LavinMQ
  # Configuration for a specific SNI hostname. Only the certificate and mTLS
  # settings can differ per host: the TLS version is selected before the
  # servername callback swaps the context, and the listener's cipher settings
  # are passed in when a context is built, as TLS 1.2 picks its cipher after.
  class SNIHost
    Log = LavinMQ::Log.for "sni"

    property hostname : String

    # Default TLS settings (used by all protocols unless overridden)
    property tls_cert : String = ""
    property tls_key : String = ""
    property? tls_verify_peer : Bool = false
    property tls_ca_cert : String = ""
    property tls_keylog_file : String = ""

    # AMQP-specific overrides (nil/empty means use the default)
    property amqp_tls_cert : String? = nil
    property amqp_tls_key : String? = nil
    property amqp_tls_verify_peer : Bool? = nil
    property amqp_tls_ca_cert : String? = nil
    property amqp_tls_keylog_file : String? = nil

    # MQTT-specific overrides (nil/empty means use the default)
    property mqtt_tls_cert : String? = nil
    property mqtt_tls_key : String? = nil
    property mqtt_tls_verify_peer : Bool? = nil
    property mqtt_tls_ca_cert : String? = nil
    property mqtt_tls_keylog_file : String? = nil

    # HTTP-specific overrides (nil/empty means use the default)
    property http_tls_cert : String? = nil
    property http_tls_key : String? = nil
    property http_tls_verify_peer : Bool? = nil
    property http_tls_ca_cert : String? = nil
    property http_tls_keylog_file : String? = nil

    @amqp_tls_context : OpenSSL::SSL::Context::Server?
    @mqtt_tls_context : OpenSSL::SSL::Context::Server?
    @http_tls_context : OpenSSL::SSL::Context::Server?

    def initialize(@hostname : String)
    end

    # Get or create the TLS context for AMQP
    def amqp_tls_context(ciphers : String, ciphersuites : String) : OpenSSL::SSL::Context::Server
      @amqp_tls_context ||= create_tls_context(
        pick(@amqp_tls_cert, @tls_cert),
        pick(@amqp_tls_key, @tls_key),
        {@amqp_tls_verify_peer, @tls_verify_peer}.find(&.is_a?(Bool)),
        pick(@amqp_tls_ca_cert, @tls_ca_cert),
        pick(@amqp_tls_keylog_file, @tls_keylog_file),
        ciphers, ciphersuites
      )
    end

    # Get or create the TLS context for MQTT
    def mqtt_tls_context(ciphers : String, ciphersuites : String) : OpenSSL::SSL::Context::Server
      @mqtt_tls_context ||= create_tls_context(
        pick(@mqtt_tls_cert, @tls_cert),
        pick(@mqtt_tls_key, @tls_key),
        {@mqtt_tls_verify_peer, @tls_verify_peer}.find(&.is_a?(Bool)),
        pick(@mqtt_tls_ca_cert, @tls_ca_cert),
        pick(@mqtt_tls_keylog_file, @tls_keylog_file),
        ciphers, ciphersuites
      )
    end

    # Get or create the TLS context for HTTP
    def http_tls_context(ciphers : String, ciphersuites : String) : OpenSSL::SSL::Context::Server
      @http_tls_context ||= create_tls_context(
        pick(@http_tls_cert, @tls_cert),
        pick(@http_tls_key, @tls_key),
        {@http_tls_verify_peer, @tls_verify_peer}.find(&.is_a?(Bool)),
        pick(@http_tls_ca_cert, @tls_ca_cert),
        pick(@http_tls_keylog_file, @tls_keylog_file),
        ciphers, ciphersuites
      )
    end

    def tls_context(protocol : Symbol, ciphers : String, ciphersuites : String) : OpenSSL::SSL::Context::Server
      case protocol
      when :amqp then amqp_tls_context(ciphers, ciphersuites)
      when :mqtt then mqtt_tls_context(ciphers, ciphersuites)
      when :http then http_tls_context(ciphers, ciphersuites)
      else            raise ArgumentError.new("unknown protocol #{protocol}")
      end
    end

    # An empty override is kept: an empty key means the key is in the cert file
    private def pick(override : String?, base : String) : String
      override || base
    end

    private def create_tls_context(cert_path, key_path, verify_peer, ca_cert, keylog_file, ciphers, ciphersuites) : OpenSSL::SSL::Context::Server
      context = OpenSSL::SSL::Context::Server.new

      # TLS 1.2 picks its cipher after the servername callback, from this
      # context, so it must not keep OpenSSL's default cipher list
      context.ciphers = ciphers unless ciphers.empty?
      context.cipher_suites = ciphersuites unless ciphersuites.empty?

      # Set certificate and key
      context.certificate_chain = cert_path
      context.private_key = key_path.empty? ? cert_path : key_path

      # Set client certificate verification (mTLS)
      if verify_peer
        context.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT
        unless ca_cert.empty?
          if File.directory?(ca_cert)
            context.ca_certificates_path = ca_cert
          else
            context.ca_certificates = ca_cert
          end
        end
      else
        context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
      end

      # Set keylog file
      keylog_file = ENV.fetch("SSLKEYLOGFILE", "") if keylog_file.empty?
      context.keylog_file = keylog_file unless keylog_file.empty?

      context
    end
  end

  # Manager for SNI host configurations
  class SNIManager
    Log = LavinMQ::Log.for "sni"

    getter hosts : Hash(String, SNIHost)
    getter wildcard_hosts : Hash(String, SNIHost)

    def initialize
      @hosts = Hash(String, SNIHost).new
      @wildcard_hosts = Hash(String, SNIHost).new
    end

    def add_host(host : SNIHost)
      if host.hostname.starts_with?("*.")
        # Store wildcard without the "*" prefix, e.g. "*.example.com" -> ".example.com"
        @wildcard_hosts[host.hostname[1..]] = host
      else
        @hosts[host.hostname] = host
      end
      Log.info { "Registered SNI host: #{host.hostname}" }
    end

    def get_host(hostname : String) : SNIHost?
      # Try exact match first, then wildcard by suffix
      @hosts[hostname]? || get_wildcard_host(hostname)
    end

    # Lookup by exact section name only, no wildcard resolution.
    # Used during config parsing to find a previously registered host.
    def get_exact_host(hostname : String) : SNIHost?
      if hostname.starts_with?("*.")
        @wildcard_hosts[hostname[1..]]?
      else
        @hosts[hostname]?
      end
    end

    private def get_wildcard_host(hostname : String) : SNIHost?
      # Find the first dot and look up the suffix (e.g. "foo.example.com" -> ".example.com")
      if dot_idx = hostname.index('.')
        @wildcard_hosts[hostname[dot_idx..]]?
      end
    end

    def empty?
      @hosts.empty? && @wildcard_hosts.empty?
    end
  end
end
