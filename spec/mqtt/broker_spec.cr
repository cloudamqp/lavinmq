require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  def self.connect_packet(client_id, clean_session = true)
    MQTT::Protocol::Connect.new(
      client_id: client_id,
      clean_session: clean_session,
      keepalive: 60u16,
      username: "guest",
      password: nil,
      will: nil,
    )
  end

  describe LavinMQ::MQTT::Broker do
    it "does not run a client whose session was deleted before it attached" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        reader, writer = IO.pipe
        done = Channel(Nil).new
        spawn do
          # Stands in for a same client_id CONNECT deleting the session while
          # this one is still writing its CONNACK.
          broker.run_client(MQTT::Protocol::IO::V3.new(reader), LavinMQ::ConnectionInfo.local,
            server.users["guest"], connect_packet("a")) { vhost.delete_queue("mqtt.a") }
          done.send nil
        end
        select
        when done.receive
        when timeout(1.second)
          fail "run_client ran a client without a session"
        end
        vhost.@connections.@connections.should be_empty
      ensure
        reader.try &.close
        writer.try &.close
      end
    end

    it "takes over a persistent connection that is still sending its CONNACK [MQTT-3.1.4-2]" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        user = server.users["guest"]
        a_reader, a_writer = IO.pipe
        b_reader, b_writer = IO.pipe
        a_done = Channel(Nil).new
        b_client = nil
        spawn do
          broker.run_client(MQTT::Protocol::IO::V3.new(a_reader), LavinMQ::ConnectionInfo.local,
            user, connect_packet("a", clean_session: false)) do
            # B connects in full while A is still writing its CONNACK.
            spawn do
              broker.run_client(MQTT::Protocol::IO::V3.new(b_reader), LavinMQ::ConnectionInfo.local,
                user, connect_packet("a", clean_session: false)) { }
            end
            wait_for { b_client = vhost.session?("mqtt.a").try(&.client) }
          end
          a_done.send nil
        end
        select
        when a_done.receive
        when timeout(1.second)
          fail "the earlier connection kept running alongside the later one"
        end
        vhost.session("mqtt.a").client.should be b_client
        vhost.@connections.@connections.should eq [b_client]
      ensure
        {a_reader, a_writer, b_reader, b_writer}.each { |io| io.try &.close }
      end
    end

    it "takes over a connection whose CONNECT is still declaring its session [MQTT-3.1.4-2]" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        user = server.users["guest"]
        lock = vhost.@definitions.not_nil!.@definitions_lock
        a_reader, a_writer = IO.pipe
        b_reader, b_writer = IO.pipe
        a_done = Channel(Nil).new
        # Parks A inside `Sessions#declare`, before it is registered.
        lock.lock
        spawn do
          broker.run_client(MQTT::Protocol::IO::V3.new(a_reader), LavinMQ::ConnectionInfo.local,
            user, connect_packet("a", clean_session: false)) { }
          a_done.send nil
        end
        Fiber.yield
        spawn do
          broker.run_client(MQTT::Protocol::IO::V3.new(b_reader), LavinMQ::ConnectionInfo.local,
            user, connect_packet("a", clean_session: false)) { }
        end
        Fiber.yield
        lock.unlock
        select
        when a_done.receive
        when timeout(1.second)
          fail "the earlier connection kept running alongside the later one"
        end
        b_client = broker.@clients["a"]
        vhost.session("mqtt.a").client.should be b_client
        vhost.@connections.@connections.should eq [b_client]
        broker.@client_locks.should be_empty
      ensure
        {a_reader, a_writer, b_reader, b_writer}.each { |io| io.try &.close }
      end
    end

    it "gives a clean takeover a new session while the old one is still being deleted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        user = server.users["guest"]
        lock = vhost.@definitions.not_nil!.@definitions_lock
        c_reader, c_writer = IO.pipe
        n_reader, n_writer = IO.pipe
        n_done = Channel(Nil).new(1)
        spawn do
          broker.run_client(MQTT::Protocol::IO::V3.new(c_reader), LavinMQ::ConnectionInfo.local,
            user, connect_packet("a")) { }
        end
        wait_for { vhost.session?("mqtt.a").try &.client }
        old_session = vhost.session("mqtt.a")
        # Parks the old connection's `Session#delete` in `@vhost.delete_queue`,
        # after it has marked the session deleted.
        lock.lock
        spawn do
          broker.run_client(MQTT::Protocol::IO::V3.new(n_reader), LavinMQ::ConnectionInfo.local,
            user, connect_packet("a")) { }
          n_done.send nil
        end
        sleep 50.milliseconds
        lock.unlock
        wait_for(1.second) { vhost.session?("mqtt.a").try { |s| s != old_session && s.client } }
        n_client = broker.@clients["a"]
        vhost.session("mqtt.a").client.should be n_client
        # The old connection's `remove_client` may still be waiting to run.
        wait_for(1.second) { broker.@client_locks.empty? }
        select
        when n_done.receive
          fail "the new connection was closed after CONNACK"
        else
        end
      ensure
        {c_reader, c_writer, n_reader, n_writer}.each { |io| io.try &.close }
      end
    end

    it "counts a connection that failed at CONNACK as created and closed once" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        created_before = vhost.connection_created_count
        closed_before = vhost.connection_closed_count
        reader, writer = IO.pipe
        expect_raises(IO::Error) do
          broker.run_client(MQTT::Protocol::IO::V3.new(reader), LavinMQ::ConnectionInfo.local,
            server.users["guest"], connect_packet("a")) { raise IO::Error.new("CONNACK write failed") }
        end
        server.update_stats_rates
        vhost.connection_created_count.should eq created_before + 1
        vhost.connection_closed_count.should eq closed_before + 1
        vhost.@connections.@connections.should be_empty
        broker.@client_locks.should be_empty
      ensure
        reader.try &.close
        writer.try &.close
      end
    end

    it "counts a connection that was taken over as closed once" do
      with_server do |server|
        vhost = server.vhosts["/"]
        closed_before = vhost.connection_closed_count
        with_client_io(server) do |io|
          connect(io, client_id: "a")
          with_client_io(server) do |io2|
            connect(io2, client_id: "a")
            io.should be_closed
            disconnect(io2)
          end
        end
        wait_for { vhost.@connections.@connections.empty? }
        server.update_stats_rates
        vhost.connection_closed_count.should eq closed_before + 2
      end
    end

    it "does not deadlock when a session is deleted while its client waits on the definitions lock" do
      with_server do |server|
        vhost = server.vhosts["/"]
        lock = vhost.@definitions.not_nil!.@definitions_lock
        with_client_io(server) do |io|
          connect(io, client_id: "a")
          client = server.mqtt_server.broker("/").@clients["a"]
          holding = Channel(Nil).new
          go = Channel(Nil).new
          deleted = Channel(Nil).new
          # An HTTP API queue delete: `DefinitionsStore#apply` calls
          # `Session#delete` with the lock held.
          spawn do
            lock.synchronize do
              holding.send nil
              go.receive
              vhost.delete_queue("mqtt.a")
            end
            deleted.send nil
          end
          holding.receive
          # Parks the client's read fiber on the lock, in `bind_queue`.
          subscribe(io, expect_response: false, topic_filters: mk_topic_filters({"a/b", 0}))
          sleep 50.milliseconds
          go.send nil
          select
          when deleted.receive
          when timeout(2.seconds)
            # Break the cycle so the server can shut down.
            client.@waitgroup.done
            fail "Session#delete waited on a client that waits on the lock it holds"
          end
        end
      end
    end
  end
end
