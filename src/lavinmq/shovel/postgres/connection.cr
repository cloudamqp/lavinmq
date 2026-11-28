require "socket"
require "openssl"
require "openssl/hmac"
require "base64"
require "digest/md5"
require "digest/sha256"
require "random/secure"

module LavinMQ
  module Shovel
    module Postgres
      # An ErrorResponse from the server, or a protocol violation.
      class Error < Exception
        getter code : String?

        def initialize(message : String, @code : String? = nil)
          super(message)
        end
      end

      # Log sequence numbers are 64 bit WAL positions, written as two 32 bit
      # hex halves: "16/B374D848".
      module LSN
        def self.format(lsn : UInt64) : String
          "#{(lsn >> 32).to_s(16).upcase}/#{(lsn & 0xFFFFFFFF_u64).to_s(16).upcase}"
        end

        def self.parse(str : String) : UInt64
          hi, lo = str.split('/', 2)
          (hi.to_u64(16) << 32) | lo.to_u64(16)
        rescue ArgumentError | IndexError
          raise Error.new("Invalid LSN: #{str}")
        end
      end

      # PostgreSQL timestamps are microseconds since 2000-01-01 UTC.
      EPOCH = Time.utc(2000, 1, 1)

      def self.to_time(micros : Int64) : Time
        EPOCH + micros.microseconds
      end

      def self.from_time(time : Time) : Int64
        (time - EPOCH).total_microseconds.to_i64
      end

      # Double quotes an identifier, e.g. a publication or slot name.
      def self.quote_ident(name : String) : String
        %("#{name.gsub('"', %(""))}")
      end

      # Single quotes a string literal.
      def self.quote_literal(value : String) : String
        "'#{value.gsub('\'', "''")}'"
      end

      # SCRAM-SHA-256 client (RFC 5802/7677), without channel binding, as
      # PostgreSQL implements it: the username in the exchange is ignored.
      class SCRAM
        getter client_first_bare : String

        def initialize(@password : String, @nonce : String = Random::Secure.base64(18), username = "")
          @client_first_bare = "n=#{username},r=#{@nonce}"
          @server_signature = Bytes.empty
        end

        def client_first : String
          "n,,#{@client_first_bare}"
        end

        def client_final(server_first : String) : String
          attrs = parse(server_first)
          nonce = attrs["r"]? || raise Error.new("SCRAM: missing nonce")
          raise Error.new("SCRAM: server nonce mismatch") unless nonce.starts_with?(@nonce)
          salt = Base64.decode(attrs["s"]? || raise Error.new("SCRAM: missing salt"))
          iterations = (attrs["i"]? || raise Error.new("SCRAM: missing iterations")).to_i
          salted = OpenSSL::PKCS5.pbkdf2_hmac(@password, salt, iterations,
            algorithm: OpenSSL::Algorithm::SHA256, key_size: 32)
          client_key = OpenSSL::HMAC.digest(:sha256, salted, "Client Key")
          stored_key = Digest::SHA256.digest(client_key)
          without_proof = "c=biws,r=#{nonce}"
          auth_message = "#{@client_first_bare},#{server_first},#{without_proof}"
          client_signature = OpenSSL::HMAC.digest(:sha256, stored_key, auth_message)
          proof = Bytes.new(client_key.size) { |i| client_key[i] ^ client_signature[i] }
          server_key = OpenSSL::HMAC.digest(:sha256, salted, "Server Key")
          @server_signature = OpenSSL::HMAC.digest(:sha256, server_key, auth_message)
          "#{without_proof},p=#{Base64.strict_encode(proof)}"
        end

        def verify!(server_final : String) : Nil
          attrs = parse(server_final)
          if err = attrs["e"]?
            raise Error.new("SCRAM: #{err}")
          end
          v = attrs["v"]? || raise Error.new("SCRAM: missing server signature")
          unless Base64.decode(v) == @server_signature
            raise Error.new("SCRAM: server signature mismatch")
          end
        end

        private def parse(msg : String) : Hash(String, String)
          msg.split(',').each_with_object(Hash(String, String).new) do |attr, h|
            k, _, v = attr.partition('=')
            h[k] = v
          end
        end
      end

      # A minimal PostgreSQL frontend: startup, authentication (cleartext, MD5,
      # SCRAM-SHA-256), TLS, simple queries and the streaming replication
      # sub-protocol (CopyBoth). Only one fiber may read; writes must be
      # serialized by the caller.
      class Connection
        PROTOCOL_VERSION =   196608_i32
        SSL_REQUEST      = 80877103_i32

        SSL_MODES = {"disable", "prefer", "require", "verify-ca", "verify-full"}
        # The oldest server version replication connections are made to
        MIN_SERVER_VERSION = 14

        getter parameters = Hash(String, String).new

        # The URI's sslmode, `prefer` by default. An unknown value is an error,
        # so a typo never silently turns off certificate verification.
        def self.sslmode(uri : URI) : String
          mode = uri.query_params["sslmode"]? || "prefer"
          unless SSL_MODES.includes?(mode)
            raise Error.new("Unsupported sslmode '#{mode}', use one of #{SSL_MODES.join(", ")}")
          end
          # verify-ca checks no host name, so with the system CAs any
          # certificate they ever issued, for any host, would be accepted
          if mode == "verify-ca" && !uri.query_params["sslrootcert"]?.presence
            raise Error.new("sslmode=verify-ca requires sslrootcert, or use sslmode=verify-full")
          end
          mode
        end

        # The URI path is percent-encoded; the database defaults to the user name.
        def self.database(uri : URI, user : String) : String
          URI.decode(uri.path.lchop('/')).presence || user
        end

        # `timeout` bounds the TCP connect and every read and write until the
        # session is ready, so a server that accepts the connection but never
        # answers (TLS, authentication) can't block a start forever. The
        # replication stream itself has no read timeout: it can be idle.
        def self.connect(uri : URI, application_name = "lavinmq", replication = true,
                         timeout : Time::Span = 30.seconds) : self
          sslmode = sslmode(uri)
          # hostname, not host: an IPv6 address without its brackets
          host = uri.hostname.presence || "localhost"
          port = uri.port || 5432
          tcp = TCPSocket.new(host, port, connect_timeout: timeout)
          tcp.read_timeout = timeout
          tcp.write_timeout = timeout
          tcp.sync = false
          tcp.keepalive = true
          tcp.tcp_nodelay = true
          io = begin
            negotiate_tls(tcp, uri, host, sslmode)
          rescue ex
            tcp.close
            raise ex
          end
          conn = new(io)
          begin
            conn.startup(uri, application_name, replication)
          rescue ex
            conn.close
            raise ex
          end
          tcp.read_timeout = nil
          tcp.write_timeout = nil
          conn
        rescue ex : Socket::Error | IO::Error | OpenSSL::Error
          raise Error.new("Could not connect to #{host}:#{port}: #{ex.message}")
        end

        private def self.negotiate_tls(tcp : TCPSocket, uri : URI, host : String, sslmode : String) : IO
          return tcp if sslmode == "disable"
          tcp.write_bytes(8_i32, IO::ByteFormat::BigEndian)
          tcp.write_bytes(SSL_REQUEST, IO::ByteFormat::BigEndian)
          tcp.flush
          case tcp.read_byte
          when 'S'.ord
            ctx = tls_context(uri.query_params["sslrootcert"]?.presence)
            case sslmode
            when "verify-full"
              OpenSSL::SSL::Socket::Client.new(tcp, ctx, sync_close: true, hostname: host)
            when "verify-ca"
              OpenSSL::SSL::Socket::Client.new(tcp, ctx, sync_close: true)
            when "prefer", "require"
              ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
              OpenSSL::SSL::Socket::Client.new(tcp, ctx, sync_close: true, hostname: host)
            else
              raise Error.new("Unsupported sslmode '#{sslmode}'")
            end
          when 'N'.ord
            raise Error.new("Server does not support TLS (sslmode=#{sslmode})") unless sslmode == "prefer"
            tcp
          else
            tcp.close
            raise Error.new("Unexpected response to SSLRequest")
          end
        end

        # A context that trusts the system CAs, or, with sslrootcert, only that
        # file, like libpq: Context::Client.new loads the system CAs, and
        # ca_certificates= would only add the file to them.
        private def self.tls_context(rootcert : String?) : OpenSSL::SSL::Context::Client
          return OpenSSL::SSL::Context::Client.new unless rootcert
          ctx = OpenSSL::SSL::Context::Client.insecure
          ctx.add_options(OpenSSL::SSL::Options.flags(ALL, NO_TLS_V1, NO_TLS_V1_1,
            NO_SESSION_RESUMPTION_ON_RENEGOTIATION, NO_RENEGOTIATION))
          ctx.add_modes(OpenSSL::SSL::Modes.flags(AUTO_RETRY, RELEASE_BUFFERS))
          ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER
          ctx.default_verify_param = "ssl_server"
          ctx.ca_certificates = rootcert
          ctx
        end

        def initialize(@io : IO)
          @buf = Bytes.new(8192)
        end

        def close : Nil
          @io.close unless @io.closed?
        rescue IO::Error | OpenSSL::Error
        end

        def closed? : Bool
          @io.closed?
        end

        def startup(uri : URI, application_name : String, replication : Bool) : Nil
          user = uri.user.presence || raise Error.new("A user is required in the PostgreSQL URI")
          # pgoutput sends text in the client encoding; the JSON body must be UTF-8.
          # Its values are formatted by this session's settings, so don't let the
          # server's default DateStyle change them.
          params = {"user" => user, "database" => Connection.database(uri, user),
                    "application_name" => application_name, "client_encoding" => "UTF8",
                    "DateStyle" => "ISO"}
          params["replication"] = "database" if replication
          body = IO::Memory.new
          body.write_bytes(PROTOCOL_VERSION, IO::ByteFormat::BigEndian)
          params.each do |k, v|
            write_cstring(body, k)
            write_cstring(body, v)
          end
          body.write_byte 0_u8
          @io.write_bytes((body.size + 4).to_i32, IO::ByteFormat::BigEndian)
          @io.write body.to_slice
          @io.flush
          authenticate(user, uri.password || "")
          wait_for_ready
          check_server_version! if replication
        end

        # The server's major version, from the server_version it reports at
        # startup, e.g. "16.4 (Ubuntu 16.4-1)" or "17beta1".
        def server_version : Int32?
          @parameters["server_version"]?.try { |v| v[/\A\d+/]?.try &.to_i }
        end

        private def check_server_version! : Nil
          version = server_version
          return if version && version >= MIN_SERVER_VERSION
          close
          raise Error.new("PostgreSQL #{MIN_SERVER_VERSION} or later is required, " \
                          "the server is #{@parameters["server_version"]? || "an unknown version"}")
        end

        private def authenticate(user : String, password : String) : Nil
          scram = nil
          loop do
            type, payload = read_message
            case type
            when 'R'
              code = IO::ByteFormat::BigEndian.decode(Int32, payload[0, 4])
              case code
              when 0 # AuthenticationOk
                return
              when 3 # AuthenticationCleartextPassword
                send_message('p') { |io| write_cstring(io, password) }
              when 5 # AuthenticationMD5Password
                salt = payload[4, 4]
                inner = Digest::MD5.hexdigest("#{password}#{user}")
                outer = Digest::MD5.hexdigest do |ctx|
                  ctx.update inner
                  ctx.update salt
                end
                send_message('p') { |io| write_cstring(io, "md5#{outer}") }
              when 10 # AuthenticationSASL
                mechanisms = String.new(payload[4..]).split('\0', remove_empty: true)
                unless mechanisms.includes?("SCRAM-SHA-256")
                  raise Error.new("Unsupported SASL mechanisms: #{mechanisms.join(", ")}")
                end
                s = scram = SCRAM.new(password)
                first = s.client_first
                send_message('p') do |io|
                  write_cstring(io, "SCRAM-SHA-256")
                  io.write_bytes(first.bytesize.to_i32, IO::ByteFormat::BigEndian)
                  io.write first.to_slice
                end
              when 11 # AuthenticationSASLContinue
                s = scram || raise Error.new("Unexpected SASLContinue")
                final = s.client_final(String.new(payload[4..]))
                send_message('p', &.write(final.to_slice))
              when 12 # AuthenticationSASLFinal
                s = scram || raise Error.new("Unexpected SASLFinal")
                s.verify!(String.new(payload[4..]))
              else
                raise Error.new("Unsupported authentication method (#{code})")
              end
            when 'E'
              raise error_from(payload)
            else
              raise Error.new("Unexpected message during authentication: #{type}")
            end
          end
        end

        private def wait_for_ready : Nil
          loop do
            type, payload = read_message
            case type
            when 'Z' then return
            when 'E' then raise error_from(payload)
            when 'S'
              k, _, v = String.new(payload).chomp('\0').partition('\0')
              @parameters[k] = v
            end
            # BackendKeyData ('K') and NoticeResponse ('N') are ignored
          end
        end

        # Runs a simple query and returns all rows of its (last) result as text.
        def query(sql : String) : Array(Array(String?))
          send_message('Q') { |io| write_cstring(io, sql) }
          rows = Array(Array(String?)).new
          error = nil
          loop do
            type, payload = read_message
            case type
            when 'T' then rows.clear
            when 'D' then rows << parse_data_row(payload)
            when 'E' then error = error_from(payload)
            when 'Z'
              raise error if error
              return rows
            end
            # CommandComplete, EmptyQueryResponse, notices etc. are ignored
          end
        end

        # Issues START_REPLICATION for a pgoutput slot and waits for the server
        # to enter CopyBoth mode. A start LSN of 0 resumes from the slot's
        # confirmed_flush_lsn.
        def start_replication(slot : String, publication : String, lsn : UInt64 = 0_u64) : Nil
          pubs = Postgres.quote_literal(Postgres.quote_ident(publication))
          sql = "START_REPLICATION SLOT #{Postgres.quote_ident(slot)} LOGICAL #{LSN.format(lsn)} " \
                "(proto_version '1', publication_names #{pubs})"
          send_message('Q') { |io| write_cstring(io, sql) }
          loop do
            type, payload = read_message
            case type
            when 'W' then return # CopyBothResponse
            when 'E'
              error = error_from(payload)
              wait_for_ready
              raise error
            end
          end
        end

        # Reads the next CopyData payload. Returns nil when the server ends the
        # copy. The returned slice is only valid until the next read.
        def read_copy_data : Bytes?
          loop do
            type, payload = read_message
            case type
            when 'd' then return payload
            when 'c' then return # CopyDone
            when 'E' then raise error_from(payload)
            end
            # NoticeResponse, ParameterStatus etc. are ignored
          end
        end

        # Standby status update: tells the server how far the stream has been
        # received, and how far it has been durably handled (flushed). The
        # slot's confirmed_flush_lsn follows `flush`.
        def send_standby_status(write : UInt64, flush : UInt64, apply : UInt64, reply = false) : Nil
          send_message('d') do |io|
            io.write_byte 'r'.ord.to_u8
            io.write_bytes(write, IO::ByteFormat::BigEndian)
            io.write_bytes(flush, IO::ByteFormat::BigEndian)
            io.write_bytes(apply, IO::ByteFormat::BigEndian)
            io.write_bytes(Postgres.from_time(Time.utc), IO::ByteFormat::BigEndian)
            io.write_byte(reply ? 1_u8 : 0_u8)
          end
        end

        def send_copy_done : Nil
          send_message('c') { }
        end

        private def send_message(type : Char, &)
          body = IO::Memory.new
          yield body
          @io.write_byte type.ord.to_u8
          @io.write_bytes((body.size + 4).to_i32, IO::ByteFormat::BigEndian)
          @io.write body.to_slice
          @io.flush
        end

        private def read_message : {Char, Bytes}
          type = @io.read_byte || raise IO::EOFError.new("Connection closed by server")
          len = @io.read_bytes(Int32, IO::ByteFormat::BigEndian) - 4
          raise Error.new("Invalid message length #{len}") if len < 0
          @buf = Bytes.new(Math.pw2ceil(len)) if len > @buf.size
          payload = @buf[0, len]
          @io.read_fully(payload)
          {type.unsafe_chr, payload}
        end

        private def parse_data_row(payload : Bytes) : Array(String?)
          io = IO::Memory.new(payload, writable: false)
          count = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
          Array(String?).new(count) do
            len = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
            len < 0 ? nil : io.read_string(len)
          end
        end

        private def error_from(payload : Bytes) : Error
          fields = Hash(Char, String).new
          io = IO::Memory.new(payload, writable: false)
          while (code = io.read_byte) && code != 0
            fields[code.unsafe_chr] = io.gets('\0', chomp: true) || ""
          end
          Error.new("#{fields['S']? || "ERROR"}: #{fields['M']? || "unknown error"}", fields['C']?)
        end

        private def write_cstring(io : IO, str : String)
          raise Error.new("String contains NUL byte") if str.includes?('\0')
          io.write str.to_slice
          io.write_byte 0_u8
        end
      end
    end
  end
end
