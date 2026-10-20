require "socket"
require "../config"
require "../proxy_protocol"

module LavinMQ
  module Clustering
    class Proxy
      Log = LavinMQ::Log.for "clustering.proxy"
      @proxy_header = false
      @local_address : String

      def initialize(bind_host, bind_port)
        @server = s = TCPServer.new(bind_host, bind_port)
        @local_address = s.local_address.to_s
      end

      def initialize(path : String)
        File.delete?(path)
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
        set_socket_opts(client) if client.is_a? TCPSocket
        proxy_header = leader_proxy_header(client)
        target = TCPSocket.new(target_host, target_port)
        set_socket_opts(target)
        target.write_bytes proxy_header, IO::ByteFormat::NetworkEndian if proxy_header
        spawn(name: "Proxy client copy loop") do
          IO.copy(target, client)
        rescue IO::Error
        ensure
          target.close rescue nil
          client.close rescue nil
        end
        IO.copy(client, target)
      rescue IO::Error
      rescue ex : ProxyProtocol::Error | ArgumentError
        Log.warn { "Invalid PROXY header: #{ex.message}" }
      ensure
        target.try &.close rescue nil
        client.close rescue nil
      end

      # The PROXY header to put in front of the connection to the leader, if any
      private def leader_proxy_header(client) : ProxyProtocol::V1?
        return unless @proxy_header
        return unless client.is_a? TCPSocket
        return if forward_proxy_header?(client)
        ProxyProtocol::V1.new(client.remote_address, client.local_address)
      end

      # A trusted load balancer's PROXY header is forwarded as is, instead of
      # our own, so that the leader, which trusts followers, sees the client's
      # address. A header from an untrusted source is dropped, like the leader
      # would do.
      private def forward_proxy_header?(client : TCPSocket) : Bool
        config = Config.instance
        return false unless config.tcp_proxy_protocol?
        return false unless ProxyProtocol.header?(client)
        return true if config.trusted_proxy_source?(client.remote_address.address)
        Log.warn { "PROXY protocol from untrusted source #{client.remote_address}, ignoring header" }
        ProxyProtocol.parse(client)
        false
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
