require "socket"
require "../config"
require "../accept_loop"
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
        AcceptLoop.each(@server, "Proxy listener") do |socket|
          spawn handle_client(socket, target_host, target_port), name: "Handle proxy client"
        end
      end

      def close
        @server.close
      end

      private def handle_client(client, target_host, target_port)
        # A client that stops reading must not keep the connection open: the
        # copy loop would block in its write forever, and never see that the
        # leader closed the connection after its own tcp_send_timeout
        client.write_timeout = Config.instance.tcp_send_timeout.seconds
        proxy_header = nil
        if client.is_a? TCPSocket
          set_socket_opts(client)
          proxy_header = proxy_header_for(client) if @proxy_header
        end
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
      rescue ex
        Log.warn { "Error proxying connection: #{ex.inspect}" }
      ensure
        target.try &.close rescue nil
        client.close rescue nil
      end

      # The leader reads a single PROXY header, so a header the client sent
      # (e.g. from a load balancer) can't be forwarded in front of ours. With
      # tcp_proxy_protocol it's consumed here, and forwarded instead of ours if
      # the source is trusted, like the leader would have done. A v2 header is
      # forwarded as v2, so that its TLVs (e.g. TLS details) are kept.
      private def proxy_header_for(client : TCPSocket) : ProxyProtocol::V1 | ProxyProtocol::V2
        config = Config.instance
        if config.tcp_proxy_protocol? && (header = ProxyProtocol.read(client))
          return header if config.trusted_proxy_source?(client.remote_address.address)
          Log.warn { "PROXY protocol from untrusted source #{client.remote_address}, ignoring header" }
        end
        ProxyProtocol::V1.new(client.remote_address, client.local_address)
      end

      private def set_socket_opts(socket : TCPSocket)
        socket.sync = true
        socket.read_buffering = false
        socket.tcp_nodelay = true
        return if socket.remote_address.loopback?

        Config.instance.apply_tcp_keepalive(socket)
      end
    end
  end
end
