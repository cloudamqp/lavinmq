require "http/server"
require "openssl"
require "./handler"
require "../server"
require "../config"

module LavinMQ
  module SQS
    Log = LavinMQ::Log.for "sqs"

    # The SQS listener: a dedicated HTTP server (disabled by default,
    # conventionally port 9324) so queue URLs are self-contained and
    # management authentication does not apply. Per-vhost state lives in
    # `SQS::Broker`, owned and closed by each `VHost`.
    class Server
      Log = LavinMQ::Log.for "sqs.server"

      def initialize(@server : LavinMQ::Server, @config : Config = Config.instance)
        @closed = false
        @bound = false
        handlers = [
          (::HTTP::LogHandler.new(log: Log) if Log.level == ::Log::Severity::Debug),
          Handler.new(@server.users, @server.vhosts, @config),
        ].select(::HTTP::Handler)
        @http = ::HTTP::Server.new(handlers)
      end

      def bound? : Bool
        @bound
      end

      def bind_tcp(address : String, port : Int)
        addr = @http.bind_tcp address, port
        @bound = true
        Log.info { "Bound SQS to #{addr}" }
        addr
      rescue ex : Socket::BindError
        abort "Error: #{ex.message}"
      end

      def bind_tls(address : String, port : Int, ctx : OpenSSL::SSL::Context::Server)
        addr = @http.bind_tls address, port, ctx
        @bound = true
        Log.info { "Bound SQS to #{addr} (TLS)" }
        addr
      rescue ex : Socket::BindError
        abort "Error: #{ex.message}"
      end

      def bind_unix(path : String)
        File.delete?(path)
        addr = @http.bind_unix(path)
        File.chmod(path, 0o666)
        @bound = true
        Log.info { "Bound SQS to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def close
        return if @closed
        @closed = true
        @http.close rescue nil
      end
    end
  end
end
