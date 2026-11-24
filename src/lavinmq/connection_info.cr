require "socket"
require "openssl"
require "../stdlib/openssl_peer_verified"

module LavinMQ
  class ConnectionInfo
    getter remote_address : IPAddress
    getter local_address : IPAddress
    property? ssl : Bool = false
    property? ssl_verify : Bool = false
    property ssl_version : String?
    property ssl_cipher : String?
    property ssl_key_alg : String?
    property ssl_sig_alg : String?
    property ssl_cn : String?
    property ssl_san_entries : Array(SubjectAlternativeName)?
    # True when the addresses come from a PROXY protocol header
    getter? proxied : Bool
    property? tls_terminated : Bool = false

    # Remote and local addresses from the server's perspective
    def initialize(remote_address, local_address, *, @proxied : Bool = false)
      @remote_address = IPAddress.new(remote_address)
      @local_address = IPAddress.new(local_address)
    end

    # A proxied address describes the client as seen by the proxy, not a connection on this host
    def loopback? : Bool
      @remote_address.loopback? && !@proxied
    end

    def self.local
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      new(src, dst)
    end

    def with_ssl(ssl_client : OpenSSL::SSL::Socket) : self
      @tls_terminated = true
      self.ssl = true
      self.ssl_version = ssl_client.tls_version
      self.ssl_cipher = ssl_client.cipher
      self.ssl_key_alg = nil
      self.ssl_sig_alg = nil
      cert = ssl_client.peer_certificate if ssl_client.peer_certificate_verified?
      self.ssl_verify = !cert.nil?
      self.ssl_cn = extract_common_name(cert)
      self.ssl_san_entries = extract_subject_alternative_name_entries(cert)
      self
    end

    def extract_subject_alternative_name_entries(peer_certificate) : Array(SubjectAlternativeName)?
      return unless peer_certificate
      san_ext = peer_certificate.extensions.find { |ext| ext.oid == "subjectAltName" }
      return unless san_ext
      san_ext.value.split(',').map do |entry|
        type, _, value = entry.strip.partition(':')
        SubjectAlternativeName.new(type, value)
      end
    end

    def extract_common_name(peer_certificate) : String?
      return unless peer_certificate
      peer_certificate.subject.to_a.find { |(key, _)| key == "CN" }.try &.last
    end

    record SubjectAlternativeName, type : String, value : String

    # Suspecting memory problem with Socket::IPAddress in Crystal 1.15.0
    struct IPAddress
      private IPV4_MAPPED_IPV6_PREFIX = "::ffff:"

      getter address : String
      getter port : UInt16
      # Only ConnectionInfo#loopback? may read this, it also accounts for PROXY headers
      protected getter? loopback : Bool

      def initialize(ip_address : Socket::IPAddress)
        @address = unmap_ipv6(ip_address.address)
        @port = ip_address.port.to_u16!
        @loopback = ip_address.loopback?
      end

      def to_s(io)
        io << @address << ':' << @port
      end

      private def unmap_ipv6(address : String) : String
        return address unless address.starts_with?(IPV4_MAPPED_IPV6_PREFIX)

        address.byte_slice(IPV4_MAPPED_IPV6_PREFIX.bytesize)
      end
    end
  end
end
