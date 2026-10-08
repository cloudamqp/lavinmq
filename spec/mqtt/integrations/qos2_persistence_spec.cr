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
          log = session.@packet_id_log.not_nil!
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            step_drain(gate) # syncs the subscriber's segment
            wait_for { log_size(log) > 4 }
            server.persister.last_sync.not_nil!.paths.should_not contain log.@path
            io.should be_silent # no PUBREC before the id is durable
            step_drain(gate)    # syncs the log
            read_packet(io).as(MQTT::Protocol::PubRec).packet_id.should eq 7u16
            server.persister.last_sync.not_nil!.paths.should contain log.@path
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
          log = session.@packet_id_log.not_nil!
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
          log = session.@packet_id_log.not_nil!
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "a/b", payload: "2".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # both routed, the PUBACK ahead of the barrier
            disconnect(io)
            wait_for { session.client.nil? }
            release_drain(gate)
          end
          wait_for { log_size(log) == 4 + 3 }
        end
      end
    end

    it "records a held packet id queued behind an acknowledgement that fails with a non-IO error" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          session = server.vhosts["/"].session("mqtt.pub")
          log = session.@packet_id_log.not_nil!
          client = session.client.not_nil!
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "a/b", payload: "2".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # both routed, the PUBACK ahead of the barrier
            pointerof(client.@io).value = MQTT::Protocol::IO.new(NonIOErrorWriteIO.new(client.@io.io))
            release_drain(gate)
          end
          wait_for { log_size(log) == 4 + 3 }
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
          log = session.@packet_id_log.not_nil!
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # routed, the barrier waits in the old writer
            with_client_io(server) do |io2|
              connect(io2, client_id: "pub", clean_session: false) # takeover
              release_drain(gate)
              wait_for { log_size(log) == 4 + 3 }
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
        log = session.@packet_id_log.not_nil!
        session.publish_received(7u16).should be_true
        stale = session.publish_routed(7u16).not_nil!
        session.pubrel_received(7u16).should be_true
        session.publish_received(7u16).should be_true
        current = session.publish_routed(7u16).not_nil!
        size = log_size(log) # the PUBREL_RECEIVED record
        session.record_publish_received(7u16, stale)
        log_size(log).should eq size # its routing may not be durable yet
        session.record_publish_received(7u16, current)
        log_size(log).should eq size + 3
        session.record_publish_received(7u16, current)
        log_size(log).should eq size + 3
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
          log = session.@packet_id_log.not_nil!
          with_drain_held do
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            pingpong(io) # routed, the barrier is still queued in the client
            session.close
            File.size(log.@path).should eq 4 + 3 # closed, so truncated to it
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
          puback(sub_io, pub.packet_id.not_nil!)
          pingpong(sub_io)
          %w[mqtt.sub mqtt.pub].each do |name|
            path = server.vhosts["/"].session(name).@packet_id_log.not_nil!.@path
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
          log = server.vhosts["/"].session("mqtt.pub").@packet_id_log.not_nil!
          with_drain_held do |gate|
            (1u16..3u16).each do |id|
              publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: id, expect_response: false)
            end
            pingpong(io) # all three are routed
            step_drain(gate)
            # All three ids recorded after the first drain, not one per drain
            wait_for { log_size(log) == 4 + 3 * 3 }
            step_drain(gate)
            Array.new(3) { read_packet(io).as(MQTT::Protocol::PubRec).packet_id }.should eq [1u16, 2u16, 3u16]
          end
        end
      end
    end

    it "closes the connection when the packet id log cannot be written" do
      with_server do |server|
        offline_subscriber(server)
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          # In the way of the file the first record creates
          Dir.mkdir(server.vhosts["/"].session("mqtt.pub").@packet_id_log.not_nil!.@path)
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

    it "deletes the log with the session" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub", clean_session: false)
          publish(io, topic: "a/b", qos: 2u8, packet_id: 7u16)
          disconnect(io)
        end
        session = server.vhosts["/"].session("mqtt.pub")
        path = session.@packet_id_log.not_nil!.@path
        File.exists?(path).should be_true
        server.vhosts["/"].delete_queue("mqtt.pub")
        File.exists?(path).should be_false
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
      pubrec(io, pub.packet_id.not_nil!)
      read_packet(io).should be_a(MQTT::Protocol::PubRel)
      pubcomp(io, pub.packet_id.not_nil!)
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
          resent.packet_id.should eq sent.not_nil!.packet_id
          resent.dup?.should be_true
          resent.payload.should eq sent.not_nil!.payload
        end
      end
    end

    it "re-sends the PUBREL owed after PUBREC, and not the message" do
      id = 0u16
      with_server(clean_dir: false) do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          id = deliver_with_id_past_one(server, io).packet_id.not_nil!
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
          pubrec(io, pub.packet_id.not_nil!)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          pubcomp(io, pub.packet_id.not_nil!)
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

    it "syncs the ack file of the PUBREC'd message before the PUBREL" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          pub = deliver_with_id_past_one(server, io, count: 1)
          pubrec(io, pub.packet_id.not_nil!)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          server.persister.last_sync.not_nil!.paths.any?(&.ends_with?("acks.0000000001")).should be_true
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
          msg_dir = server.vhosts["/"].session("mqtt.sub").@msg_store.@msg_dir
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            2.times { |i| publish_qos2(pub_io, (i + 1).to_u16, topic: "a/b") }
          end
          first = read_publish(io)
          read_publish(io)
          pubrec(io, first.packet_id.not_nil!)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          File.exists?(File.join(msg_dir, "msgs.0000000001")).should be_false
          server.persister.last_sync.not_nil!.paths.should contain msg_dir
        end
      end
    ensure
      LavinMQ::Config.instance.segment_size = segment_size.not_nil!
    end

    # Invariant guard: unchanged code writes no record for the `wait_for`.
    # Without the wait the PUBLISH goes to the old connection and its requeue
    # gives the same result. Protects the requeue `client=` does while the
    # deliver_loop waits.
    it "sends the PUBLISH once when the subscriber reconnects while it waits to be durable" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.not_nil!
          with_drain_held do |gate|
            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
            end
            wait_for { log_size(log) == 4 + 11 } # the deliver_loop waits
            with_client_io(server) do |io2|
              connect(io2, client_id: "sub", clean_session: false) # takeover
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
      deliver_loops = -> do
        count = 0
        Fiber.each { |f| count += 1 if f.name == "Session#deliver_loop" && !f.dead? }
        count
      end
      loops = 0
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.not_nil!
          with_drain_held do |gate|
            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b", qos: 2u8, packet_id: 1u16, expect_response: false)
              disconnect(pub_io)
            end
            wait_for { log_size(log) == 4 + 11 } # the deliver_loop waits
            loops = deliver_loops.call
            server.vhosts["/"].delete_queue("mqtt.sub")
            release_drain(gate)
          end
          wait_for { deliver_loops.call == loops - 1 }
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
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", clean_session: false)
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          log = server.vhosts["/"].session("mqtt.sub").@packet_id_log.not_nil!
          server.persister.close
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            3.times { |i| publish(pub_io, topic: "a/b", qos: 2u8, packet_id: (i + 1).to_u16, expect_response: false) }
            pingpong(pub_io)
          end
          wait_for { log_size(log) >= 4 + 11 }
          sub_io.should be_silent
          # One record, not one per message the deliver_loop walked past
          log_size(log).should eq 4 + 11
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
