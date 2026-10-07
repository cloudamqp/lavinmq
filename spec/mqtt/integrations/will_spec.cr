require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "client will" do
    it "is not delivered on graceful disconnect [MQTT-3.14.4-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"#", 0})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
            disconnect(io2)
          end

          # If the will has been published it should be received before this
          publish(io, topic: "a/b", payload: "alive".to_slice)

          pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
          pub.payload.should eq("alive".to_slice)
          pub.topic.should eq("a/b")

          disconnect(io)
        end
      end
    end

    it "is not delivered when a PUBREL arrives for an unknown packet id" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"will/t", 0}))

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 30u16)

            # A PUBREL naming an id the broker is not holding is ordinary: the
            # held ids do not survive a restart, so every resuming QoS 2
            # publisher sends one. If it raised, read_loop would treat it as a
            # lost connection and publish this client's will [MQTT-3.1.2-8].
            pubrel(io2, 99u16)
            read_packet(io2).should be_a(MQTT::Protocol::PubComp)
            pingpong(io2)
            disconnect(io2)
          end

          read_packet(io).should be_nil

          disconnect(io)
        end
      end
    end

    describe "is delivered on ungraceful disconnect" do
      it "when client unexpected closes tcp connection" do
        with_server do |server|
          with_client_io(server) do |io|
            connect(io)
            topic_filters = mk_topic_filters({"will/t", 0})
            subscribe(io, topic_filters: topic_filters)

            with_client_io(server) do |io2|
              will = MQTT::Protocol::Will.new(
                topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
              connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
            end

            pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
            pub.payload.should eq("dead".to_slice)
            pub.topic.should eq("will/t")

            disconnect(io)
          end
        end
      end

      it "when server closes connection because protocol error" do
        with_server do |server|
          with_client_io(server) do |io|
            connect(io)
            topic_filters = mk_topic_filters({"will/t", 0})
            subscribe(io, topic_filters: topic_filters)

            with_client_io(server) do |io2|
              will = MQTT::Protocol::Will.new(
                topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
              connect(io2, client_id: "will_client", will: will, keepalive: 20u16)

              broken_packet_io = IO::Memory.new
              publish(MQTT::Protocol::IO.v3(broken_packet_io), topic: "foo", qos: 1u8, expect_response: false)
              broken_packet = broken_packet_io.to_slice
              broken_packet[0] |= 0b0000_0110u8 # set both qos bits to 1
              io2.io.write broken_packet
            end

            pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
            pub.payload.should eq("dead".to_slice)
            pub.topic.should eq("will/t")

            disconnect(io)
          end
        end
      end
    end

    it "can be retained [MQTT-3.1.2-15]" do
      with_server do |server|
        with_client_io(server) do |io2|
          will = MQTT::Protocol::Will.new(
            topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: true)
          connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
        end

        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"will/t", 0})
          subscribe(io, topic_filters: topic_filters)

          pub = read_packet(io).should be_a(MQTT::Protocol::Publish)
          pub.payload.should eq("dead".to_slice)
          pub.topic.should eq("will/t")
          pub.retain?.should be_true

          disconnect(io)
        end
      end
    end

    it "won't be published if missing permission" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"topic-without-permission/t", 0})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
          end

          # Send a ping to ensure we can read at least one packet, so we're not stuck
          # waiting here (since this spec verifies that nothing is sent)
          ping(io)

          pkt = read_packet(io)
          pkt.should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "qos can't be set of will flag is unset [MQTT-3.1.2-11]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO.v3(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0001_0000u8
          io.io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end

    it "qos must not be 3 [MQTT-3.1.2-12]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          will = MQTT::Protocol::Will.new(
            topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
          connect(MQTT::Protocol::IO.v3(temp_io), will: will, client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0001_1000u8
          io.io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end

    it "carries the Will Properties onto the published message" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_socket(server) do |dying_socket|
            dying = MQTT::Protocol::IO.v5(dying_socket)
            props = MQTT::Protocol::WillProperties.new
            props.payload_format_indicator = true
            props.message_expiry_interval = 120u32
            props.content_type = "text/plain"
            props.response_topic = "reply/here"
            props.correlation_data = "cid".to_slice
            props.user_properties = [{"a", "1"}, {"b", "2"}]
            # Server behaviour, not wire content: must not reach the subscriber.
            props.will_delay_interval = 0u32
            will = MQTT::Protocol::Will.new(topic: "will/t", payload: "bye".to_slice,
              qos: 1u8, retain: false, properties: props)
            connect(dying, version: MQTT::Protocol::Version::V5,
              client_id: "dying", will: will)
            # 0x04 publishes the will without an error path [MQTT-3.14.4-3]
            MQTT::Protocol::Disconnect.new(
              MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage).to_io(dying)
            dying.flush
          end

          pub = read_packet(sub).as(MQTT::Protocol::Publish)
          pub.topic.should eq "will/t"
          String.new(pub.payload).should eq "bye"
          pub.properties.payload_format_indicator?.should be_true
          pub.properties.message_expiry_interval.should eq 120u32
          pub.properties.content_type.should eq "text/plain"
          pub.properties.response_topic.should eq "reply/here"
          String.new(pub.properties.correlation_data.not_nil!).should eq "cid"
          pub.properties.user_properties.should eq [{"a", "1"}, {"b", "2"}]
        end
      end
    end

    it "keeps Will user property order and duplicate keys [MQTT-3.1.3-10]" do
      # The reason they are an array of {key, value} tables rather than a flat
      # table: a Hash would lose both.
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_socket(server) do |dying_socket|
            dying = MQTT::Protocol::IO.v5(dying_socket)
            props = MQTT::Protocol::WillProperties.new
            props.user_properties = [{"k", "1"}, {"k", "2"}, {"a", "3"}]
            will = MQTT::Protocol::Will.new(topic: "will/t", payload: "x".to_slice,
              qos: 1u8, retain: false, properties: props)
            connect(dying, version: MQTT::Protocol::Version::V5,
              client_id: "dying", will: will)
            MQTT::Protocol::Disconnect.new(
              MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage).to_io(dying)
            dying.flush
          end

          pub = read_packet(sub).as(MQTT::Protocol::Publish)
          pub.properties.user_properties.should eq [{"k", "1"}, {"k", "2"}, {"a", "3"}]
        end
      end
    end

    it "drops the Will Properties cleanly for a v3 subscriber" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_socket(server) do |dying_socket|
            dying = MQTT::Protocol::IO.v5(dying_socket)
            props = MQTT::Protocol::WillProperties.new
            props.content_type = "text/plain"
            props.user_properties = [{"a", "1"}]
            will = MQTT::Protocol::Will.new(topic: "will/t", payload: "bye".to_slice,
              qos: 1u8, retain: false, properties: props)
            connect(dying, version: MQTT::Protocol::Version::V5,
              client_id: "dying", will: will)
            MQTT::Protocol::Disconnect.new(
              MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage).to_io(dying)
            dying.flush
          end

          pub = read_packet(sub).as(MQTT::Protocol::Publish)
          String.new(pub.payload).should eq "bye"
          pub.properties.content_type.should be_nil
          pub.properties.user_properties.should be_empty
        end
      end
    end

    it "accepts a v5 Will at QoS 2" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          will = MQTT::Protocol::Will.new(topic: "will/t", payload: "x".to_slice,
            qos: 2u8, retain: false)
          connect(io, false, version: MQTT::Protocol::Version::V5,
            client_id: "qos2will", will: will)
          io.flush
          connack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::Success
        end
      end
    end

    it "delivers a v3 Will at the lower of its QoS and the subscription's" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_io(server) do |dying|
            will = MQTT::Protocol::Will.new(topic: "will/t", payload: "bye".to_slice,
              qos: 2u8, retain: false)
            connect(dying, client_id: "dying", will: will)
            dying.io.close # ungraceful, so the will fires
          end

          pub = read_packet(sub).as(MQTT::Protocol::Publish)
          String.new(pub.payload).should eq "bye"
          pub.qos.should eq 1u8
        end
      end
    end

    it "No Local suppresses a will sent to the dying client's own session" do
      # The will's publisher is the connection that died, so [MQTT-3.8.3-3]
      # applies to it like any other publish. Worth pinning down because the
      # ordering is not obvious: a takeover closes the previous connection
      # (publishing its will) while that connection's session is still
      # attached and still holds the no_local binding, so the will is dropped.
      with_server do |server|
        will = MQTT::Protocol::Will.new(
          topic: "last/words", payload: "bye".to_slice, qos: 1u8, retain: false)

        props = MQTT::Protocol::ConnectProperties.new
        props.session_expiry_interval = 3600u32
        with_client_socket(server) do |first_socket|
          first = MQTT::Protocol::IO.v5(first_socket)
          connect(first, version: MQTT::Protocol::Version::V5, client_id: "sub",
            clean_session: false, will: will, properties: props)
          subscribe(first, topic_filters: [subtopic("last/words", 1u8, no_local: true)])

          # A second connection with the same client id takes over, which closes
          # the first and publishes its will.
          with_client_socket(server) do |second_socket|
            second = MQTT::Protocol::IO.v5(second_socket)
            connect(second, version: MQTT::Protocol::Version::V5, client_id: "sub",
              clean_session: false, properties: props)
            second.should be_drained
          end
        end
      end
    end

    it "retain can't be set of will flag is unset [MQTT-3.1.2-13]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO.v3(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0010_0000u8
          io.io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end
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
