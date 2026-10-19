require "socket"
require "amqp-client"
require "./config"
require "./cluster_monitor"

module Booth
  # Opens client connections to whichever node answers. A node that lost
  # power never sends a FIN or RST, so connections rely on short read
  # timeouts (fed by heartbeats) to notice it and reconnect elsewhere.
  class Connector
    CONNECT_TIMEOUT   = 1.second
    HANDSHAKE_TIMEOUT = 3.seconds
    # The server sends a heartbeat after half of this many seconds of silence
    HEARTBEAT = 2_u16
    # Read timeout, so a dead peer is noticed within about this long
    DEAD_PEER_TIMEOUT = 3.seconds

    getter clients_per_host = Hash(String, Int32).new(0)
    @rr = 0

    def initialize(@config : Config, @monitor : ClusterMonitor)
    end

    # Blocks until connected, trying the leader first. Yields the connection
    # and the host it went to, and returns when the connection is gone.
    def with_amqp(name : String, & : AMQP::Client::Connection, String -> _) : Nil
      @monitor.hosts_leader_first.each do |host|
        conn = begin
          connect_amqp(host, name)
        rescue ex
          Log.debug { "#{name}: #{host}: #{ex.message}" }
          next
        end
        track(host) do
          yield conn, host
        ensure
          conn.close rescue nil
        end
        return
      end
      sleep 200.milliseconds # no node answered
    end

    # Spreads connections over all nodes rather than going to the leader:
    # the IoT devices should show what happens to clients of each node.
    def next_host : String
      hosts = @monitor.online_hosts
      hosts = @config.nodes.map(&.host) if hosts.empty?
      hosts[(@rr += 1) % hosts.size]
    end

    def track(host : String, &)
      @clients_per_host[host] += 1
      begin
        yield
      ensure
        @clients_per_host[host] -= 1
      end
    end

    def connect_amqp(host : String, name : String) : AMQP::Client::Connection
      socket = TCPSocket.new(host, @config.amqp_port, connect_timeout: CONNECT_TIMEOUT)
      socket.tcp_nodelay = true
      socket.sync = false
      socket.read_buffering = true
      socket.buffer_size = 16_384
      conn = with_deadline(socket, HANDSHAKE_TIMEOUT) do
        info = AMQP::Client::ConnectionInformation.new(name: name)
        AMQP::Client::Connection.start(socket, @config.user, @config.password, @config.vhost,
          1024_u16, 131_072_u32, HEARTBEAT, info, name)
      end
      # Connection.start leaves the socket without a read timeout
      socket.read_timeout = DEAD_PEER_TIMEOUT
      conn
    end

    def connect_mqtt(host : String, client_id : String, clean_session = true) : MqttClient
      socket = TCPSocket.new(host, @config.mqtt_port, connect_timeout: CONNECT_TIMEOUT)
      socket.tcp_nodelay = true
      socket.sync = false
      socket.read_timeout = HANDSHAKE_TIMEOUT
      client = MqttClient.new(socket)
      client.connect(client_id, @config.user, @config.password, clean_session)
      socket.read_timeout = DEAD_PEER_TIMEOUT
      client
    rescue ex
      socket.try &.close rescue nil
      raise ex
    end

    # Closes the socket if the block doesn't finish in time, as a server
    # that went away mid-handshake would otherwise hang it for a minute
    private def with_deadline(socket, timeout, &)
      done = ::Channel(Nil).new
      spawn(name: "handshake deadline") do
        select
        when done.receive?
        when timeout(timeout)
          socket.close rescue nil
        end
      end
      begin
        yield
      ensure
        done.close
      end
    end
  end
end
