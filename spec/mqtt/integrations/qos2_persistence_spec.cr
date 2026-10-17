require "../spec_helper"

# Reads from the socket, fails every write with an error that is not an
# IO::Error, like the OpenSSL::SSL::Error a TLS socket raises
private class NonIOErrorWriteIO < IO
  class Error < Exception; end

  def initialize(@io : IO)
  end

  def read(slice : Bytes)
    @io.read(slice)
  end

  def write(slice : Bytes) : Nil
    raise Error.new("write failed")
  end

  def close
    @io.close
  end

  def closed?
    @io.closed?
  end
end

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # The log is created by its first record. While open its file is
  # capacity-sized, so this is the logical size.
  def self.log_size(log : LavinMQ::MQTT::PacketIdLog) : Int64
    log.@mfile.try(&.size) || 0i64
  end

  # A durable subscriber that is offline, so messages are stored for it
  def self.offline_subscriber(server, topic = "a/b", qos = 1u8)
    with_client_io(server) do |io|
      connect(io, client_id: "sub", clean_session: false)
      subscribe(io, topic_filters: mk_topic_filters({topic, qos}))
      disconnect(io)
    end
  end

  describe "inbound QoS 2 across a broker restart" do
    it "does not route a PUBLISH re-sent after a restart again [MQTT-4.3.3-2]" do
      with_server(clean_dir: false) do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
          pubrel(io, 7u16)
          read_packet(io).should be_a(MQTT::Protocol::PubComp)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          read_publish(io).payload.should eq "1".to_slice
          io.should be_silent
        end
      end
    end

    # Invariant guard: passes before the log exists, protects the release path
    it "routes a reused packet id after a restart once it was released" do
      with_server(clean_dir: false) do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish_qos2(io, 7u16, topic: "a/b", payload: "1".to_slice)
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish_qos2(io, 7u16, topic: "a/b", payload: "2".to_slice)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          Array.new(2) { String.new(read_publish(io).payload) }.should eq ["1", "2"]
        end
      end
    end

    it "writes the packet id only after the routed message is durable" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            step_drain(gate) # syncs the subscriber's segment
            wait_for { log_size(log) > 4 }
            last_sync = server.persister.last_sync.should_not be_nil
            last_sync.paths.should_not contain log.@path
            io.should be_silent # no PUBREC before the id is durable
            step_drain(gate)    # syncs the log
            read_packet(io).as(MQTT::Protocol::PubRec).packet_id.should eq 7u16
            last_sync = server.persister.last_sync.should_not be_nil
            last_sync.paths.should contain log.@path
          end
        end
      end
    end

    it "records a held packet id when the publisher disconnects before its PUBREC" do
      with_server(clean_dir: false) do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # the PUBLISH is routed
            disconnect(io)
            wait_for { session.client.nil? }
            release_drain(gate)
          end
          wait_for { log_size(log) > 4 }
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
          pubrel(io, 7u16)
          read_packet(io).should be_a(MQTT::Protocol::PubComp)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          read_publish(io).payload.should eq "1".to_slice
          io.should be_silent
        end
      end
    end

    it "records a held packet id queued behind an acknowledgement the closed socket drops" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "a/b", payload: "2".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # both routed, the PUBACK ahead of the barrier
            disconnect(io)
            wait_for { session.client.nil? }
            release_drain(gate)
          end
          wait_for { log_size(log) == 4 + 4 }
        end
      end
    end

    it "records a held packet id queued behind an acknowledgement that fails with a non-IO error" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          client = session.client.should_not be_nil
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "a/b", payload: "2".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # both routed, the PUBACK ahead of the barrier
            pointerof(client.@io).value = MQTT::Protocol::IO.new(NonIOErrorWriteIO.new(client.@io.io))
            release_drain(gate)
          end
          wait_for { log_size(log) == 4 + 4 }
          io.should be_closed
        end
      end
    end

    it "records a held packet id from the old connection's writer after a takeover" do
      with_server(clean_dir: false) do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # routed, the barrier waits in the old writer
            with_client_io(server) do |io2|
              connect(io2, client_id: "pub", clean_session: false) # takeover
              release_drain(gate)
              wait_for { log_size(log) == 4 + 4 }
              # The PUBREC is the old connection's; the new one gets nothing
              io2.should be_silent
              disconnect(io2)
            end
          end
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
          pubrel(io, 7u16)
          read_packet(io).should be_a(MQTT::Protocol::PubComp)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          read_publish(io).payload.should eq "1".to_slice
          io.should be_silent
        end
      end
    end

    # The old writer's barrier outlives a takeover: the client releases the
    # id on the new connection and reuses it before that barrier fires.
    it "records a reused packet id only for the routing its barrier was queued for" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          disconnect(io)
        end
        session = server.vhosts["/"].session("mqtt.pub")
        log = session.@packet_id_log.should_not be_nil
        session.publish_received(7u16).should be_true
        stale = session.publish_routed(7u16).should_not be_nil
        session.pubrel_received(7u16).should be_true
        session.publish_received(7u16).should be_true
        current = session.publish_routed(7u16).should_not be_nil
        # Never recorded, so its release writes nothing either
        log_size(log).should eq 0
        session.record_publish_received(7u16, stale)
        log_size(log).should eq 0 # its routing may not be durable yet
        session.record_publish_received(7u16, current)
        log_size(log).should eq 4 + 4
        session.record_publish_received(7u16, current)
        log_size(log).should eq 4 + 4
      end
    end

    it "records a held packet id when the broker shuts down before its PUBREC" do
      gate = ::Channel(Nil).new
      io = nil
      begin
        with_server(clean_dir: false) do |server|
          offline_subscriber(server)
          pub_io = io = with_client_io(server)
          connect(pub_io, client_id: "pub", clean_session: false)
          LavinMQ::Persister.drain_gate = gate
          publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
          pingpong(pub_io) # routed, the barrier still waits for the drain
        end                # a graceful shutdown, with the drain still held
      ensure
        release_drain(gate)
        io.try &.close
      end
      with_server do |server|
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub", clean_session: false)
          publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
          pubrel(pub_io, 7u16)
          read_packet(pub_io).should be_a(MQTT::Protocol::PubComp)
          disconnect(pub_io)
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          read_publish(sub_io).payload.should eq "1".to_slice
          sub_io.should be_silent
        end
      end
    end

    it "records a held packet id when the session closes under a connected publisher" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          with_drain_held do
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # routed, the barrier is still queued in the client
            session.close
            File.size(log.@path).should eq 4 + 4 # closed, so truncated to it
          end
        end
      end
    end

    it "creates no log for a durable session that only uses QoS 0 and 1" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub", clean_session: false)
            publish(pub_io, topic: "a/b", payload: "0".to_slice, qos: 0u8, expect_response: false)
            publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16)
            disconnect(pub_io)
          end
          read_publish(sub_io)
          pub = read_publish(sub_io)
          puback(sub_io, pub.packet_id.should_not be_nil)
          pingpong(sub_io)
          %w[mqtt.sub mqtt.pub].each do |name|
            log = server.vhosts["/"].session(name).@packet_id_log.should_not be_nil
            path = log.@path
            File.exists?(path).should be_false
          end
        end
      end
    end

    it "costs two drains for a pipelined burst of QoS 2 publishes" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          log = server.vhosts["/"].session("mqtt.pub").@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            (1u16..3u16).each do |id|
              publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: id, expect_response: false)
            end
            pingpong(io) # all three are routed
            step_drain(gate)
            # All three ids recorded after the first drain, not one per drain
            wait_for { log_size(log) == 4 + 3 * 4 }
            step_drain(gate)
            Array.new(3) { read_packet(io).as(MQTT::Protocol::PubRec).packet_id }.should eq [1u16, 2u16, 3u16]
          end
        end
      end
    end

    it "closes a session whose packet id log cannot be loaded, and loads the rest" do
      path = ""
      with_server(clean_dir: false) do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          disconnect(io)
        end
        path = server.vhosts["/"].session("mqtt.pub").@packet_id_log.should_not(be_nil).@path
      end
      # Unreadable as a log
      File.delete(path)
      Dir.mkdir(path)
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.session("mqtt.pub").closed?.should be_true
        vhost.session("mqtt.sub").closed?.should be_false
        with_client_io(server) do |io|
          connack = connect(io, client_id: "pub", clean_session: false).as(MQTT::Protocol::Connack)
          connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::ServerUnavailable
        end
        # A clean CONNECT replaces it
        with_client_io(server) do |io|
          connack = connect(io, client_id: "pub", clean_session: true).as(MQTT::Protocol::Connack)
          connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          disconnect(io)
        end
      end
    end

    it "closes a session whose message store closes while loading" do
      dir = ""
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
          end
          # Sent and recorded, never PUBREC'd
          read_publish(io)
          disconnect(io)
        end
        dir = server.vhosts["/"].session("mqtt.sub").@data_dir
      end
      # An unknown schema version makes the store close itself
      File.open(File.join(dir, "msgs.0000000001"), "r+") { |f| f.write_bytes(UInt32::MAX) }
      with_server do |server|
        server.vhosts["/"].session("mqtt.sub").closed?.should be_true
        with_client_io(server) do |io|
          # Not attached, so no PUBREL for a message the store never loaded
          connack = connect(io, client_id: "sub", clean_session: false).as(MQTT::Protocol::Connack)
          connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::ServerUnavailable
        end
      end
    end

    it "keeps a packet id held when its PUBREL cannot be recorded" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.should_not be_nil
          # Makes the next append raise, as a disk error would
          pointerof(log.@closed).value = true
          pubrel(io, 7u16)
          io.should be_closed
          # Memory still agrees with the log, which holds the id
          log.awaiting_pubrel.should contain 7u16
          session.@awaiting_pubrel.should contain 7u16
        end
      end
    end

    it "closes the connection when the packet id log cannot be written" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          # In the way of the file the first record creates
          log = server.vhosts["/"].session("mqtt.pub").@packet_id_log.should_not be_nil
          Dir.mkdir_p(log.@path)
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
          io.should be_closed
        end
      end
    end

    # Invariant guard: passes before the log exists, protects the sync = false path
    it "completes the handshake with sync disabled" do
      LavinMQ::Config.instance.sync = false
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish_qos2(io, 7u16, topic: "a/b")
        end
      end
    ensure
      LavinMQ::Config.instance.sync = true
    end

    it "keeps no log for a clean session" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: true)
          publish(io, topic: "a/b", qos: 2u8, packet_id: 7u16)
          server.vhosts["/"].session("mqtt.pub").@packet_id_log.should be_nil
        end
      end
    end
  end

  # Delivers `count` QoS 2 messages to a connected "sub", completing all but
  # the last, so the last one goes out under a packet id other than 1.
  def self.deliver_with_id_past_one(server, io, count = 3)
    subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
    with_client_io(server) do |pub_io|
      connect(pub_io, client_id: "pub")
      count.times { |i| publish_qos2(pub_io, (i + 1).to_u16, topic: "a/b", payload: i.to_s.to_slice) }
      disconnect(pub_io)
    end
    # All of them first: the session sends ahead of our acknowledgements
    pubs = Array.new(count) { read_publish(io) }
    pubs[0...-1].each do |pub|
      pubrec(io, pub.packet_id.should_not be_nil)
      read_packet(io).should be_a(MQTT::Protocol::PubRel)
      pubcomp(io, pub.packet_id.should_not be_nil)
    end
    pingpong(io) # the PUBCOMPs are handled before a restart can close the session
    pubs.last
  end

  describe "outbound QoS 2 across a broker restart" do
    it "re-sends an unacknowledged PUBLISH under its original id with DUP [MQTT-4.4.0-1]" do
      sent = nil
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          sent = deliver_with_id_past_one(server, io)
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          resent = read_publish(io)
          original = sent.should_not be_nil
          resent.packet_id.should eq original.packet_id
          resent.dup?.should be_true
          resent.payload.should eq original.payload
        end
      end
    end

    it "re-sends the PUBREL owed after PUBREC, and not the message" do
      id = 0u16
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          id = deliver_with_id_past_one(server, io).packet_id.should_not be_nil
          pubrec(io, id)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq id
          io.should be_silent
        end
      end
    end

    # Invariant guard: passes before the log exists, protects the PUBCOMP record
    it "re-sends nothing after PUBCOMP" do
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          pub = deliver_with_id_past_one(server, io)
          pubrec(io, pub.packet_id.should_not be_nil)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          pubcomp(io, pub.packet_id.should_not be_nil)
          pingpong(io)
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          io.should be_silent
        end
      end
    end

    it "sends a batch of QoS 2 PUBLISHes after one drain" do
      with_server do |server|
        offline_subscriber(server, qos: 2u8)
        log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.should_not be_nil
        with_drain_held do |gate|
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            3.times { |i| publish(pub_io, topic: "a/b", qos: 2u8, packet_id: (i + 1).to_u16, expect_response: false) }
            pingpong(pub_io)
          end
          with_client_io(server) do |sub_io|
            connect(sub_io, client_id: "sub", clean_session: false)
            wait_for { log_size(log) == 4 + 3 * 12 } # all recorded, one wait
            sub_io.should be_silent
            step_drain(gate)
            3.times { read_publish(sub_io).qos.should eq 2u8 }
          end
        end
      end
    end

    it "keeps publish order across a batch that a QoS 1 message ends" do
      with_server do |server|
        offline_subscriber(server, qos: 2u8)
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          [2u8, 1u8, 2u8].each_with_index do |qos, i|
            publish(pub_io, topic: "a/b", payload: i.to_s.to_slice, qos: qos, packet_id: (i + 1).to_u16, expect_response: false)
          end
          pingpong(pub_io)
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          pubs = Array.new(3) { read_publish(sub_io) }
          pubs.map { |p| {String.new(p.payload), p.qos, p.dup?} }.should eq [{"0", 2u8, false}, {"1", 1u8, false}, {"2", 2u8, false}]
        end
      end
    end

    it "keeps re-sends under their original ids within max_inflight_messages" do
      with_server do |server|
        offline_subscriber(server, qos: 2u8)
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          5.times { |i| publish(pub_io, topic: "a/b", qos: 2u8, packet_id: (i + 1).to_u16, expect_response: false) }
          pingpong(pub_io)
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          5.times { read_publish(sub_io) }
          disconnect(sub_io)
        end
        LavinMQ::Config.instance.max_inflight_messages = 2u16
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          2.times { read_publish(sub_io).dup?.should be_true }
          sub_io.should be_silent
        end
      ensure
        LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
      end
    end

    it "sends a QoS 2 PUBLISH only once its packet id is durable" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            with_drain_held do |gate|
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
              ping(sub_io)
              read_packet(sub_io).should be_a(MQTT::Protocol::PingResp)
              sub_io.should be_silent
              release_drain(gate)
              read_publish(sub_io).qos.should eq 2u8
            end
          end
        end
      end
    end

    it "re-sends under the recorded packet id when the send fails" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          client = server.vhosts["/"].session("mqtt.sub").client.should_not be_nil
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            with_drain_held do |gate|
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
              ping(sub_io)
              read_packet(sub_io).should be_a(MQTT::Protocol::PingResp)
              # The PUBLISH_SENT record is written and waiting, so the send
              # fails after the client may already hold the id
              pointerof(client.@io).value = MQTT::Protocol::IO.new(NonIOErrorWriteIO.new(client.@io.io))
              release_drain(gate)
            end
          end
          sub_io.should be_closed
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          resent = read_publish(sub_io)
          resent.packet_id.should eq 1u16
          resent.dup?.should be_true
          disconnect(sub_io)
        end
      end
    end

    it "syncs the ack file of the PUBREC'd message before the PUBREL" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          pub = deliver_with_id_past_one(server, io, count: 1)
          pubrec(io, pub.packet_id.should_not be_nil)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          last_sync = server.persister.last_sync.should_not be_nil
          last_sync.paths.any?(&.ends_with?("acks.0000000001")).should be_true
        end
      end
    end

    # Its packet id record is synced before the send, so on power loss the
    # message must not be the part that is lost
    it "syncs a retained message replayed at QoS 2 with its packet id record" do
      with_server do |server|
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "a/b", payload: "retained".to_slice, qos: 1u8, retain: true)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          read_publish(io).qos.should eq 2u8
          last_sync = server.persister.last_sync.should_not be_nil
          last_sync.paths.any?(&.ends_with?("packet_ids.log")).should be_true
          last_sync.paths.any?(&.ends_with?("msgs.0000000001")).should be_true
        end
      end
    end

    it "syncs the data dir before the PUBREL when the PUBREC deleted the segment" do
      segment_size = LavinMQ::Config.instance.segment_size
      LavinMQ::Config.instance.segment_size = 64 # one message per segment
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          msg_dir = server.vhosts["/"].session("mqtt.sub").@data_dir
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            2.times { |i| publish_qos2(pub_io, (i + 1).to_u16, topic: "a/b") }
          end
          first = read_publish(io)
          read_publish(io)
          pubrec(io, first.packet_id.should_not be_nil)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          File.exists?(File.join(msg_dir, "msgs.0000000001")).should be_false
          last_sync = server.persister.last_sync.should_not be_nil
          last_sync.paths.should contain msg_dir
        end
      end
    ensure
      LavinMQ::Config.instance.segment_size = segment_size.should_not be_nil
    end

    it "waits for the packet id record again when re-sending after a takeover" do
      # The first record may not be durable, so the re-send must not skip it.
      # Also guards the requeue `client=` does while the deliver_loop waits:
      # the PUBLISH must go out once, on the new connection.
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.should_not be_nil
          with_drain_held do |gate|
            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
            end
            wait_for { log_size(log) == 4 + 12 } # the deliver_loop waits
            with_client_io(server) do |io2|
              connect(io2, client_id: "sub", clean_session: false) # takeover
              step_drain(gate)
              wait_for { log_size(log) == 4 + 2 * 12 } # recorded again
              io2.should be_silent
              release_drain(gate)
              resent = read_publish(io2)
              {resent.packet_id, resent.dup?}.should eq({1u16, true})
              io2.should be_silent
            end
          end
        end
      end
    end

    # Invariant guard: passes on the unfixed code, protects the delete of a
    # session whose deliver_loop waits for its packet id to be durable
    it "deletes a session whose PUBLISH waits to be durable" do
      # By identity, not by count: other specs' loops may die meanwhile
      deliver_loops = -> do
        loops = Set(Fiber).new
        Fiber.each { |f| loops << f if f.name == "Session#deliver_loop" && !f.dead? }
        loops
      end
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          session = server.vhosts["/"].session("mqtt.sub")
          log = session.@packet_id_log.should_not be_nil
          session.@deliver_loop_started.should be_false
          before = deliver_loops.call
          loop_fiber = nil
          with_drain_held do |gate|
            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
              disconnect(pub_io)
            end
            wait_for { log_size(log) == 4 + 12 } # the deliver_loop waits
            started = deliver_loops.call - before
            started.size.should eq 1
            loop_fiber = started.first
            server.vhosts["/"].delete_queue("mqtt.sub")
            release_drain(gate)
          end
          fiber = loop_fiber.should_not be_nil
          wait_for { fiber.dead? }
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish_qos2(pub_io, 2u16, topic: "a/b", payload: "2".to_slice)
          end
          pub = read_publish(sub_io)
          {pub.qos, String.new(pub.payload)}.should eq({2u8, "2"})
        end
      end
    end

    it "stops delivering QoS 2 once the persister has stopped" do
      with_server do |server|
        offline_subscriber(server, qos: 2u8)
        log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.should_not be_nil
        server.persister.close
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          3.times { |i| publish(pub_io, topic: "a/b", qos: 2u8, packet_id: (i + 1).to_u16, expect_response: false) }
          pingpong(pub_io)
        end
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          wait_for { log_size(log) >= 4 + 12 }
          sub_io.should be_silent
          # One batch of all three, and requeued messages are not recorded again
          log_size(log).should eq 4 + 3 * 12
        end
      end
    end

    it "recovers the same packet id held inbound and outbound independently" do
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "both", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          session = server.vhosts["/"].session("mqtt.both")
          pointerof(session.@last_packet_id).value = 4u16
          publish(io, topic: "a/b", payload: "x".to_slice, qos: 2u8, packet_id: 5u16, expect_response: false)
          packets = {read_packet(io), read_packet(io)}
          packets.count(&.is_a?(MQTT::Protocol::PubRec)).should eq 1
          packets.find(&.is_a?(MQTT::Protocol::Publish)).as(MQTT::Protocol::Publish).packet_id.should eq 5u16
          disconnect(io)
        end
      end
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "both", clean_session: false)
          resent = read_publish(io)
          {resent.packet_id, resent.dup?}.should eq({5u16, true})
          publish(io, topic: "a/b", payload: "x".to_slice, qos: 2u8, packet_id: 5u16, dup: true)
          pubrel(io, 5u16)
          read_packet(io).should be_a(MQTT::Protocol::PubComp)
          io.should be_silent # not routed a second time
        end
      end
    end
  end
end
