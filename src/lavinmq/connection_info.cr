require "socket"

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

    # Remote and local addresses from the server's perspective
    def initialize(remote_address, local_address)
      @remote_address = IPAddress.new(remote_address)
      @local_address = IPAddress.new(local_address)
    end
 
   def self.local
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      new(src, dst)
    end

    def with_ssl(ssl_client)
      self.ssl = true
      self.ssl_version = ssl_client.tls_version
      self.ssl_cipher = ssl_client.cipher
      self.ssl_san_entries = extract_subject_alternative_name_entries(ssl_client.peer_certificate) 
      self.ssl_cn = extract_common_name(ssl_client.peer_certificate) 
      self
    end

    def extract_subject_alternative_name_entries(peer_certificate) : Array(SubjectAlternativeName)?
      return nil unless peer_certificate
      san = peer_certificate.extensions.reject do |ext|
        ext.oid != "subjectAltName" || ext.oid != "subjectAlternativeName"
      end
      san.map_with_index do |entry, index|
        san_entry = entry.value.split(':', 2)
        SubjectAlternativeName.new(index, san_entry[0], san_entry[1])
      end
    end

    def extract_common_name(peer_certificate) : String?
      Log.debug { "extract_common_name=#{peer_certificate}"}
      return nil unless peer_certificate
      maybe_cn = peer_certificate.subject.to_a.find { |name, _value| name == "CN" }
      Log.debug { "extract_common_name#maybe_cn=#{maybe_cn}"}
      maybe_cn.try do |entry|
        Log.debug { "extract_commmon_name#entry=#{entry[0]}"}
        entry[1]
      end
    end

    struct SubjectAlternativeName
      getter index, type, value
      def initialize(@index : Int32, @type : String, @value : String)
      end
    end

    # Suspecting memory problem with Socket::IPAddress in Crystal 1.15.0
    struct IPAddress
      getter address : String
      getter port : UInt16
      getter? loopback : Bool

      def initialize(ip_address : Socket::IPAddress)
        @address = ip_address.address
        @port = ip_address.port.to_u16!
        @loopback = ip_address.loopback?
      end

      def to_s(io)
        io << @address << ':' << @port
      end
    end
  end
end
