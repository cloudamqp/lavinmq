require "openssl"

module LavinMQ
  # Configuration for a specific SNI hostname
  class SNIHost
    Log = LavinMQ::Log.for "sni"

    property hostname : String

    # Default TLS settings (used by all protocols unless overridden)
    property tls_cert : String = ""
    property tls_key : String = ""
    property tls_min_version : String = ""
    property tls_ciphers : String = ""
    property? tls_verify_peer : Bool = false
    property tls_ca_cert : String = ""
    property tls_keylog_file : String = ""

    # AMQP-specific overrides (nil/empty means use the default)
    property amqp_tls_cert : String? = nil
    property amqp_tls_key : String? = nil
    property amqp_tls_min_version : String? = nil
    property amqp_tls_ciphers : String? = nil
    property amqp_tls_verify_peer : Bool? = nil
    property amqp_tls_ca_cert : String? = nil
    property amqp_tls_keylog_file : String? = nil

    # MQTT-specific overrides (nil/empty means use the default)
    property mqtt_tls_cert : String? = nil
    property mqtt_tls_key : String? = nil
    property mqtt_tls_min_version : String? = nil
    property mqtt_tls_ciphers : String? = nil
    property mqtt_tls_verify_peer : Bool? = nil
    property mqtt_tls_ca_cert : String? = nil
    property mqtt_tls_keylog_file : String? = nil

    # HTTP-specific overrides (nil/empty means use the default)
    property http_tls_cert : String? = nil
    property http_tls_key : String? = nil
    property http_tls_min_version : String? = nil
    property http_tls_ciphers : String? = nil
    property http_tls_verify_peer : Bool? = nil
    property http_tls_ca_cert : String? = nil
    property http_tls_keylog_file : String? = nil

    getter amqp_tls_context : OpenSSL::SSL::Context::Server
    getter mqtt_tls_context : OpenSSL::SSL::Context::Server
    getter http_tls_context : OpenSSL::SSL::Context::Server

    def initialize(@hostname : String)
      @amqp_tls_context = uninitialized OpenSSL::SSL::Context::Server
      @mqtt_tls_context = uninitialized OpenSSL::SSL::Context::Server
      @http_tls_context = uninitialized OpenSSL::SSL::Context::Server
      reload
    end

    # Reload the TLS contexts (e.g., on SIGHUP)
    # Creates new contexts atomically to avoid race conditions
    # ameba:disable Metrics/CyclomaticComplexity
    def reload
      @amqp_tls_context = create_tls_context(
        @amqp_tls_cert || @tls_cert,
        @amqp_tls_key || @tls_key,
        @amqp_tls_min_version || @tls_min_version,
        @amqp_tls_ciphers || @tls_ciphers,
        {@amqp_tls_verify_peer, @tls_verify_peer}.find &.is_a?(Bool),
        @amqp_tls_ca_cert || @tls_ca_cert,
        @amqp_tls_keylog_file || @tls_keylog_file
      )
      @mqtt_tls_context = create_tls_context(
        @mqtt_tls_cert || @tls_cert,
        @mqtt_tls_key || @tls_key,
        @mqtt_tls_min_version || @tls_min_version,
        @mqtt_tls_ciphers || @tls_ciphers,
        {@mqtt_tls_verify_peer, @tls_verify_peer}.find &.is_a?(Bool),
        @mqtt_tls_ca_cert || @tls_ca_cert,
        @mqtt_tls_keylog_file || @tls_keylog_file
      )
      @http_tls_context = create_tls_context(
        @http_tls_cert || @tls_cert,
        @http_tls_key || @tls_key,
        @http_tls_min_version || @tls_min_version,
        @http_tls_ciphers || @tls_ciphers,
        {@http_tls_verify_peer, @tls_verify_peer}.find &.is_a?(Bool),
        @http_tls_ca_cert || @tls_ca_cert,
        @http_tls_keylog_file || @tls_keylog_file
      )
    end

    private def create_tls_context(cert_path, key_path, min_version, ciphers, verify_peer, ca_cert, keylog_file) : OpenSSL::SSL::Context::Server
      context = OpenSSL::SSL::Context::Server.new
      context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
      set_min_tls_version(context, min_version)
      context.certificate_chain = cert_path
      context.private_key = key_path.empty? ? cert_path : key_path
      context.ciphers = ciphers unless ciphers.empty?
      set_verify_peer(context, verify_peer, ca_cert)
      set_keylog_file(context, keylog_file)
      context
    end

    private def set_min_tls_version(context, min_version) : Nil
      case min_version
      when "1.0"
        context.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                               OpenSSL::SSL::Options::NO_TLS_V1_1 |
                               OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.1"
        context.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        context.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.2", ""
        context.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        context.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.3"
        context.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      else
        Log.warn { "Unrecognized tls_min_version for SNI host '#{@hostname}': '#{min_version}'" }
      end
    end

    private def set_verify_peer(context, verify_peer, ca_cert) : Nil
      return context.verify_mode = OpenSSL::SSL::VerifyMode::NONE unless verify_peer
      context.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT
      return if ca_cert.empty?
      if File.directory?(ca_cert)
        context.ca_certificates_path = ca_cert
      else
        context.ca_certificates = ca_cert
      end
    end

    private def set_keylog_file(context, keylog_file) : Nil
      keylog_file = ENV.fetch("SSLKEYLOGFILE", "") if keylog_file.empty?
      context.keylog_file = keylog_file unless keylog_file.empty?
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

    private def get_wildcard_host(hostname : String) : SNIHost?
      # Find the first dot and look up the suffix (e.g. "foo.example.com" -> ".example.com")
      if dot_idx = hostname.index('.')
        @wildcard_hosts[hostname[dot_idx..]]?
      end
    end

    def reload
      @hosts.each_value(&.reload)
      @wildcard_hosts.each_value(&.reload)
    end

    def clear
      @hosts.clear
      @wildcard_hosts.clear
    end

    def empty?
      @hosts.empty? && @wildcard_hosts.empty?
    end

    def each(&)
      @hosts.each_value { |host| yield host }
      @wildcard_hosts.each_value { |host| yield host }
    end
  end
end
