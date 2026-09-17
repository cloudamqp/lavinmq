require "http/server"
require "openssl"
require "./brokers"
require "./handler"
require "../server"
require "../config"

module LavinMQ
  module SQS
    Log = LavinMQ::Log.for "sqs"

    # The SQS listener: a dedicated HTTP server (default port 9324) so queue
    # URLs are self-contained and management authentication does not apply.
    class Server
      Log = LavinMQ::Log.for "sqs.server"

      getter brokers : Brokers

      def initialize(@server : LavinMQ::Server, @config : Config = Config.instance)
        @brokers = Brokers.new(@server.vhosts)
        @closed = false
        @bound = false
        handlers = [
          (::HTTP::LogHandler.new(log: Log) if Log.level == ::Log::Severity::Debug),
          Handler.new(@server.users, @brokers, @config),
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
        @brokers.close
      end
    end
  end
end
