require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  private def self.v5_connect(socket, **args)
    io = MQTT::Protocol::IO.v5(socket)
    connect(io, **{version: MQTT::Protocol::Version::V5}.merge(args))
    io
  end

  private def self.send_disconnect(io, reason : MQTT::Protocol::Disconnect::ReasonCode)
    MQTT::Protocol::Disconnect.new(reason).to_io(io)
    io.flush
  end

  private def self.will(topic = "will/t", payload = "dead")
    MQTT::Protocol::Will.new(topic: topic, payload: payload.to_slice, qos: 0u8, retain: false)
  end

  private def self.delayed_will(delay : UInt32, retain = false)
    props = MQTT::Protocol::WillProperties.new
    props.will_delay_interval = delay
    MQTT::Protocol::Will.new(topic: "will/t", payload: "dead".to_slice,
      qos: 0u8, retain: retain, properties: props)
  end

  private def self.expiry(interval : UInt32)
    props = MQTT::Protocol::ConnectProperties.new
    props.session_expiry_interval = interval
    props
  end

  # The spec sockets time out reads after 300ms, so a wait longer than that
  # polls until the deadline.
  private def self.publish_within(io, within : Time::Span) : MQTT::Protocol::Publish?
    deadline = Time.instant + within
    while Time.instant < deadline
      if pkt = read_packet(io)
        return pkt.should be_a(MQTT::Protocol::Publish)
      end
    end
  end

  describe "MQTT 5.0 PUBACK" do
    it "answers NoMatchingSubscribers when nothing is subscribed (§3.4.2.1)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket)
          publish(io, false, topic: "no/subs", qos: 1u8, packet_id: 1u16)
          io.flush
          ack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubAck)
          ack.packet_id.should eq 1u16
          ack.reason_code.should eq MQTT::Protocol::PubAck::ReasonCode::NoMatchingSubscribers
        end
      end
    end

    it "answers Success when the message matched a subscriber" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = v5_connect(sub_socket, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("has/subs", 1)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, false, topic: "has/subs", qos: 1u8, packet_id: 2u16)
            pub.flush
            ack = MQTT::Protocol::Packet.from_io(pub).as(MQTT::Protocol::PubAck)
            ack.reason_code.should eq MQTT::Protocol::PubAck::ReasonCode::Success
          end
        end
      end
    end

    it "keeps the v3 PUBACK a bare packet id on the wire" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v3(socket)
          connect(io)
          publish(io, false, topic: "no/subs", qos: 1u8, packet_id: 7u16)
          io.flush
          # 0x40, remaining length 2, packet id - no reason byte, no property
          # length. A v5-only reason code must never leak onto a v3 connection.
          buf = Bytes.new(4)
          socket.read_fully(buf)
          buf.should eq Bytes[0x40, 0x02, 0x00, 0x07]
        end
      end
    end

    it "acks the message even when the client PUBACK carries an error reason" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = v5_connect(sub_socket, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)

          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            publish(pub, topic: "a/b", qos: 1u8)
            disconnect(pub)
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          packet_id = delivered.packet_id.should_not be_nil
          MQTT::Protocol::PubAck.new(
            packet_id, MQTT::Protocol::PubAck::ReasonCode::UnspecifiedError).to_io(sub)
          sub.flush
          pingpong(sub)

          session = server.vhosts["/"].session("mqtt.sub")
          session.ack_count.should eq 1
          session.unacked_count.should eq 0
        end
      end
    end
  end

  describe "MQTT 5.0 client DISCONNECT" do
    it "publishes the will on reason 0x04 DisconnectWithWillMessage [MQTT-3.14.4-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5 = v5_connect(socket, client_id: "will_client", will: will)
            send_disconnect(v5, MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage)
          end

          pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
          pub.topic.should eq "will/t"
          pub.payload.should eq "dead".to_slice
          disconnect(io)
        end
      end
    end

    it "publishes the will on an error reason code" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5 = v5_connect(socket, client_id: "will_client", will: will)
            send_disconnect(v5, MQTT::Protocol::Disconnect::ReasonCode::UnspecifiedError)
          end

          pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
          pub.topic.should eq "will/t"
          pub.payload.should eq "dead".to_slice
          disconnect(io)
        end
      end
    end

    it "discards the will on reason 0x00 NormalDisconnection [MQTT-3.14.4-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"#", 0}))

          with_client_socket(server) do |socket|
            v5 = v5_connect(socket, client_id: "will_client", will: will)
            send_disconnect(v5, MQTT::Protocol::Disconnect::ReasonCode::NormalDisconnection)
          end

          # A published will would arrive before this sentinel
          publish(io, topic: "a/b", payload: "alive".to_slice)

          pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
          pub.topic.should eq "a/b"
          pub.payload.should eq "alive".to_slice
          disconnect(io)
        end
      end
    end
  end

  describe "MQTT 5.0 NotAuthorized reason codes" do
    before_each do
      LavinMQ::Config.instance.mqtt_permission_check_enabled = true
    end

    after_each do
      LavinMQ::Config.instance.mqtt_permission_check_enabled = false
    end

    it "answers PUBACK NotAuthorized and keeps the connection open" do
      with_server do |server|
        server.users.create("no_write", "pass")
        server.users.add_permission("no_write", "/", /.*/, /.*/, /^$/)

        with_client_socket(server) do |socket|
          io = v5_connect(socket, username: "no_write", password: "pass".to_slice)
          publish(io, false, topic: "test/topic", qos: 1u8, packet_id: 1u16)
          io.flush
          ack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubAck)
          ack.reason_code.should eq MQTT::Protocol::PubAck::ReasonCode::NotAuthorized
          # 3.3.4 lets us refuse a single PUBLISH without tearing down the session
          pingpong(io)
        end
      end
    end

    it "answers DISCONNECT NotAuthorized for a QoS 0 publish, which has no ack" do
      with_server do |server|
        server.users.create("no_write", "pass")
        server.users.add_permission("no_write", "/", /.*/, /.*/, /^$/)

        with_client_socket(server) do |socket|
          io = v5_connect(socket, username: "no_write", password: "pass".to_slice)
          publish(io, false, topic: "test/topic", qos: 0u8)
          io.flush
          disc = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Disconnect)
          disc.reason_code.should eq MQTT::Protocol::Disconnect::ReasonCode::NotAuthorized
        end
      end
    end

    it "answers SUBACK NotAuthorized per topic filter" do
      with_server do |server|
        server.users.create("no_read", "pass")
        server.users.add_permission("no_read", "/", /.*/, /^$/, /^$/)

        with_client_socket(server) do |socket|
          io = v5_connect(socket, username: "no_read", password: "pass".to_slice)
          subscribe(io, false, topic_filters: [subtopic("a/b", 0), subtopic("c/d", 1)], packet_id: 1u16)
          io.flush
          suback = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::SubAck)
          suback.packet_id.should eq 1u16
          suback.reason_codes.should eq [
            MQTT::Protocol::SubAck::ReasonCode::NotAuthorized,
            MQTT::Protocol::SubAck::ReasonCode::NotAuthorized,
          ]
        end
      end
    end
  end

  # Unlike the vhost-level refusal above, a topic rule denial never closes the
  # connection, so QoS 0 is dropped silently rather than answered with DISCONNECT.
  describe "MQTT 5.0 topic permission denial" do
    it "answers PUBACK NotAuthorized and keeps the connection open" do
      with_server do |server|
        server.vhosts["/"].mqtt_permission_service.delete("default")

        with_client_socket(server) do |socket|
          io = v5_connect(socket)
          publish(io, false, topic: "denied/t", qos: 1u8, packet_id: 1u16)
          io.flush
          ack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubAck)
          ack.packet_id.should eq 1u16
          ack.reason_code.should eq MQTT::Protocol::PubAck::ReasonCode::NotAuthorized
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)
        end
      end
    end

    it "drops a QoS 0 publish without a DISCONNECT" do
      with_server do |server|
        server.vhosts["/"].mqtt_permission_service.delete("default")

        with_client_socket(server) do |socket|
          io = v5_connect(socket)
          publish(io, false, topic: "denied/t", qos: 0u8)
          # Read exactly one packet: a DISCONNECT would arrive before the PINGRESP.
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)
        end
      end
    end
  end

  describe "MQTT 5.0 unexpected packets" do
    it "answers DISCONNECT ProtocolError (0x82) to a client-sent PINGRESP" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket)

          # PINGRESP is server-to-client only, so a client sending one is a
          # protocol error rather than something we should log a backtrace for.
          MQTT::Protocol::PingResp.new.to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "answers DISCONNECT ProtocolError (0x82) to a second CONNECT [MQTT-3.1.0-2]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket)
          connect(io, expect_response: false, version: MQTT::Protocol::Version::V5)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end
  end

  describe "MQTT 5.0 Will Delay Interval" do
    it "publishes the will once the delay has elapsed [MQTT-3.1.2-8]" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(2u32),
              properties: expiry(60u32))
          end # closed without DISCONNECT

          publish_within(watcher, 1.second).should be_nil
          pub = publish_within(watcher, 2.seconds).should_not be_nil
          pub.topic.should eq "will/t"
        end
      end
    end

    it "delays the will on DISCONNECT 0x04" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5 = v5_connect(socket, client_id: "dying", will: delayed_will(2u32),
              properties: expiry(60u32))
            send_disconnect(v5, MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage)
          end

          publish_within(watcher, 1.second).should be_nil
          publish_within(watcher, 2.seconds).should_not be_nil
        end
      end
    end

    it "does not publish the will when the client reconnects within the delay [MQTT-3.1.3-9]" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(1u32),
              properties: expiry(60u32))
          end

          # Disconnected again, normally, before the old deadline: a will
          # that was never cancelled would fire now, from the offline wait.
          with_client_socket(server) do |socket|
            v5 = v5_connect(socket, client_id: "dying", clean_session: false,
              properties: expiry(60u32))
            disconnect(v5)
          end
          publish_within(watcher, 2.seconds).should be_nil
        end
      end
    end

    it "does not publish the will on a takeover with Clean Start 0 (§3.1.4)" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |old_socket|
            v5_connect(old_socket, client_id: "dying", will: delayed_will(1u32),
              properties: expiry(60u32))
            with_client_socket(server) do |new_socket|
              v5 = v5_connect(new_socket, client_id: "dying", clean_session: false,
                properties: expiry(60u32))
              disconnect(v5)
            end
          end
          publish_within(watcher, 2.seconds).should be_nil
        end
      end
    end

    it "retains a delayed will published with the retain flag" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))
          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(1u32, retain: true),
              properties: expiry(60u32))
          end
          # `Broker#publish` stores the retained copy before routing it.
          publish_within(watcher, 3.seconds).should_not be_nil
        end

        with_client_io(server) do |late|
          connect(late, client_id: "late")
          subscribe(late, topic_filters: mk_topic_filters({"will/t", 0}))
          pub = publish_within(late, 1.second).should_not be_nil
          pub.retain?.should be_true
        end
      end
    end

    it "keeps counting the session expiry while a delayed will fires [MQTT-3.1.2-8]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          v5_connect(socket, client_id: "dying", will: delayed_will(1u32),
            properties: expiry(2u32))
        end
        closed_at = Time.instant
        wait_for { server.vhosts["/"].session?("mqtt.dying").nil? }
        # Restarting the clock when the will fires would end it at ~3s.
        (Time.instant - closed_at).should be < 2.8.seconds
      end
    end

    it "publishes the will when the session expires first [MQTT-3.1.2-8]" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(10u32),
              properties: expiry(1u32))
          end

          publish_within(watcher, 3.seconds).should_not be_nil
          server.vhosts["/"].session?("mqtt.dying").should be_nil
        end
      end
    end

    it "publishes the will at close when the session ends with the connection" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(10u32),
              properties: expiry(0u32))
          end

          publish_within(watcher, 1.second).should_not be_nil
        end
      end
    end

    it "publishes the will on a takeover with Clean Start 1 (§3.1.4)" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |old_socket|
            v5_connect(old_socket, client_id: "dying", will: delayed_will(10u32),
              properties: expiry(60u32))
            with_client_socket(server) do |new_socket|
              v5_connect(new_socket, client_id: "dying", clean_session: true,
                properties: expiry(60u32))
              publish_within(watcher, 1.second).should_not be_nil
            end
          end
        end
      end
    end

    it "publishes the will when the session is deleted" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(10u32),
              properties: expiry(60u32))
          end
          vhost = server.vhosts["/"]
          wait_for { vhost.session?("mqtt.dying").try(&.pending_will) }
          vhost.delete_queue("mqtt.dying")

          publish_within(watcher, 1.second).should_not be_nil
        end
      end
    end

    it "arms the will before a second close returns [MQTT-3.1.3-9]" do
      # A takeover cancels the will right after `close`, so every close has to
      # wait for the read fiber, not only the first one.
      with_server do |server|
        with_client_socket(server) do |socket|
          v5_connect(socket, client_id: "dying", will: delayed_will(10u32),
            properties: expiry(60u32))
          broker = server.mqtt_server.brokers["/"]?.should_not be_nil
          client = wait_for { broker.@clients["dying"]? }
          session = wait_for { server.vhosts["/"].session?("mqtt.dying").try { |s| s if s.client } }
          # Spawned before the first close wakes the read fiber, so it runs
          # first and sees the client already closed.
          armed = Channel(Bool).new(1)
          spawn do
            client.close("second")
            armed.send(!session.pending_will.nil?)
          end
          client.close("first")
          armed.receive.should be_true
        end
      end
    end

    it "publishes the will at once when its session was deleted while connected" do
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_socket(server) do |socket|
            v5_connect(socket, client_id: "dying", will: delayed_will(10u32),
              properties: expiry(60u32))
            vhost = server.vhosts["/"]
            wait_for { vhost.session?("mqtt.dying").try(&.client) }
            vhost.delete_queue("mqtt.dying")
            publish_within(watcher, 1.second).should_not be_nil
          end
        end
      end
    end

    it "keeps the expiry clock of the disconnect when a resume narrows the interval" do
      # A resume narrows the interval before `Client#run` attaches: the will
      # timer firing afterwards must not re-read it.
      with_server do |server|
        with_client_socket(server) do |socket|
          v5_connect(socket, client_id: "dying", will: delayed_will(2u32),
            properties: expiry(60u32))
        end
        vhost = server.vhosts["/"]
        session = wait_for { vhost.session?("mqtt.dying").try { |s| s if s.pending_will } }
        session.session_expiry_interval = 1u32
        sleep 3.seconds
        vhost.session?("mqtt.dying").should_not be_nil
      end
    end
  end
end
