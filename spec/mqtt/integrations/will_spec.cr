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
              publish(MQTT::Protocol::IO::V3.new(broken_packet_io), topic: "foo", qos: 1u8, expect_response: false)
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

    it "can be retained [MQTT-3.1.2-17]" do
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

    it "qos can't be set of will flag is unset [MQTT-3.1.2-13]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO::V3.new(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
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

    it "qos must not be 3 [MQTT-3.1.2-14]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          will = MQTT::Protocol::Will.new(
            topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
          connect(MQTT::Protocol::IO::V3.new(temp_io), will: will, client_id: "will_client", keepalive: 1u16, expect_response: false)
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
          sub = MQTT::Protocol::IO::V5.new(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_socket(server) do |dying_socket|
            dying = MQTT::Protocol::IO::V5.new(dying_socket)
            props = MQTT::Protocol::WillProperties.new
            props.payload_format_indicator = true
            props.message_expiry_interval = 120u32
            props.content_type = "text/plain"
            props.response_topic = "reply/here"
            props.correlation_data = "cid".to_slice
            props.user_properties = [{"a", "1"}, {"b", "2"}]
            # will_delay_interval is set but ignored for now: it is server
            # behaviour, not wire content, and must not reach the subscriber.
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
          pub.properties.payload_format_indicator.should be_true
          pub.properties.message_expiry_interval.should eq 120u32
          pub.properties.content_type.should eq "text/plain"
          pub.properties.response_topic.should eq "reply/here"
          String.new(pub.properties.correlation_data.not_nil!).should eq "cid"
          pub.properties.user_properties.should eq [{"a", "1"}, {"b", "2"}]
        end
      end
    end

    it "keeps Will user property order and duplicate keys [MQTT-3.3.2-18]" do
      # The reason they are an array of {key, value} tables rather than a flat
      # table: a Hash would lose both.
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO::V5.new(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("will/t", 1u8)])

          with_client_socket(server) do |dying_socket|
            dying = MQTT::Protocol::IO::V5.new(dying_socket)
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
            dying = MQTT::Protocol::IO::V5.new(dying_socket)
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

    it "refuses a v5 Will above maximum_qos with QoSNotSupported (0x9B)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          will = MQTT::Protocol::Will.new(topic: "will/t", payload: "x".to_slice,
            qos: 2u8, retain: false)
          connect(io, false, version: MQTT::Protocol::Version::V5,
            client_id: "qos2will", will: will)
          io.flush
          connack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::QoSNotSupported
          io.should be_closed
        end
      end
    end

    it "still accepts a v3 Will at QoS 2, clamped at delivery" do
      # Deliberate asymmetry: v3 has no return code meaning "QoS not
      # supported", so refusing would mean a misleading code or a bare close.
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
          first = MQTT::Protocol::IO::V5.new(first_socket)
          connect(first, version: MQTT::Protocol::Version::V5, client_id: "sub",
            clean_session: false, will: will, properties: props)
          subscribe(first, topic_filters: [subtopic("last/words", 1u8, no_local: true)])

          # A second connection with the same client id takes over, which closes
          # the first and publishes its will.
          with_client_socket(server) do |second_socket|
            second = MQTT::Protocol::IO::V5.new(second_socket)
            connect(second, version: MQTT::Protocol::Version::V5, client_id: "sub",
              clean_session: false, properties: props)
            second.should be_drained
          end
        end
      end
    end

    it "retain can't be set of will flag is unset [MQTT-3.1.2-15]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO::V3.new(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
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
end
