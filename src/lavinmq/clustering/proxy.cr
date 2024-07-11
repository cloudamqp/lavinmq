require "socket"
require "log"
require "../config"
require "../proxy_protocol"

module LavinMQ
  module Clustering
    class Proxy
      Log = ::Log.for("clustering.proxy")
      @proxy_header = false
      @local_address : String

      def initialize(bind_host, bind_port)
        @server = s = TCPServer.new(bind_host, bind_port)
        @local_address = s.local_address.to_s
      end

      def initialize(path : String)
        @server = s = UNIXServer.new(path)
        @local_address = s.local_address.to_s
        File.chmod(path, 0o666)
      end

      def forward_to(target_host, target_port, @proxy_header = false)
        Log.info { "Proxying from #{@local_address} to #{target_host}:#{target_port}" }
        while socket = @server.accept?
          spawn handle_client(socket, target_host, target_port), name: "Handle proxy client"
        end
      end

      def close
        @server.close
      end

      private def handle_client(client, target_host, target_port)
        target = TCPSocket.new(target_host, target_port)
        set_socket_opts(target)
        if client.is_a? TCPSocket
          set_socket_opts(client)
          if @proxy_header
            proxy_header = ProxyProtocol::V1.new(client.remote_address, client.local_address)
            target.write_bytes proxy_header, IO::ByteFormat::NetworkEndian
          end
        end
        spawn(name: "Proxy client copy loop") do
          begin
            IO.copy(target, client)
          rescue IO::Error
          ensure
            target.close rescue nil
            client.close rescue nil
          end
        end
        IO.copy(client, target)
      rescue IO::Error
      ensure
        target.try &.close rescue nil
        client.close rescue nil
      end

      private def set_socket_opts(socket : TCPSocket)
        socket.sync = true
        socket.read_buffering = false
        socket.tcp_nodelay = true
        return if socket.remote_address.loopback?

        if keepalive = Config.instance.tcp_keepalive
          socket.keepalive = true
          socket.tcp_keepalive_idle = keepalive[0]
          socket.tcp_keepalive_interval = keepalive[1]
          socket.tcp_keepalive_count = keepalive[2]
        end
      end
    end
  end
end
