require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "publish" do
    it "should return PubAck for QoS=1" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          payload = Bytes[1, 254, 200, 197, 123, 4, 87]
          ack = publish(io, topic: "test", payload: payload, qos: 1u8)
          ack.should be_a(MQTT::Protocol::PubAck)
        end
      end
    end

    it "shouldn't return anything for QoS=0" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          payload = Bytes[1, 254, 200, 197, 123, 4, 87]
          ack = publish(io, topic: "test", payload: payload, qos: 0u8)
          ack.should be_nil
        end
      end
    end

    it "sends the whole payload when the session is closed during the send" do
      with_server do |server|
        # Larger than the socket buffers, so the send blocks mid-payload
        payload = Bytes.new(32 * 1024 * 1024) { |i| (i % 251).to_u8 }
        with_client_io(server) do |sub|
          connect(sub, client_id: "slow")
          subscribe(sub, topic_filters: mk_topic_filters({"t", 0u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            publish(pub, topic: "t", payload: payload, qos: 0u8)
            disconnect(pub)
          end
          session = server.vhosts["/"].session("mqtt.slow")
          # Published asynchronously, wait for it to be stored in its own segment
          wait_for { session.@msg_store.@segments.last_value.size > payload.size }
          segment = session.@msg_store.@segments.last_value
          # Shifted, and the send is blocked as the subscriber doesn't read
          wait_for { session.message_count.zero? }
          session.close
          segment.closed?.should be_false
          packet = read_packet(sub).as(MQTT::Protocol::Publish)
          packet.payload.should eq payload
          wait_for { segment.closed? }
        end
      end
    end

    it "closes the connection on a QoS 1 PUBLISH with packet id 0 [MQTT-2.2.1-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "test", payload: "x".to_slice, qos: 1u8, packet_id: 0u16,
            expect_response: false)
          io.should be_closed
        end
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

  describe "MQTT 5.0 publish" do
    it "passes v5 PUBLISH properties through to a v5 subscriber, preserving user-property order" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("test/topic", 1)], packet_id: 1u16)

          props = MQTT::Protocol::PublishProperties.new
          props.payload_format_indicator = true
          props.message_expiry_interval = 3600u32
          props.response_topic = "reply/here"
          props.correlation_data = Bytes[1, 2, 3]
          props.content_type = "application/json"
          # order + a duplicate key, both of which must survive [MQTT-3.3.2-17/18]
          props.user_properties = [{"a", "1"}, {"b", "2"}, {"a", "3"}]

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            MQTT::Protocol::Publish.new(
              topic: "test/topic", payload: "hello".to_slice,
              packet_id: 2u16, dup: false, qos: 1u8, retain: false, properties: props,
            ).to_io(pub)
            pub.flush
            MQTT::Protocol::Packet.from_io(pub).should be_a(MQTT::Protocol::PubAck)
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          dp = delivered.properties
          dp.payload_format_indicator?.should be_true
          dp.message_expiry_interval.should eq(3600u32)
          dp.response_topic.should eq("reply/here")
          dp.correlation_data.should eq(Bytes[1, 2, 3])
          dp.content_type.should eq("application/json")
          dp.user_properties.should eq([{"a", "1"}, {"b", "2"}, {"a", "3"}])
        end
      end
    end

    it "delivers a v5-published message to a v3 subscriber without leaking v5 properties" do
      with_server do |server|
        with_client_io(server) do |sub| # v3 subscriber
          connect(sub)
          subscribe(sub, topic_filters: [subtopic("test/topic", 1)], packet_id: 1u16)

          props = MQTT::Protocol::PublishProperties.new
          props.content_type = "application/json"
          props.user_properties = [{"a", "1"}]
          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            MQTT::Protocol::Publish.new(
              topic: "test/topic", payload: "hi".to_slice,
              packet_id: 2u16, dup: false, qos: 1u8, retain: false, properties: props,
            ).to_io(pub)
            pub.flush
            MQTT::Protocol::Packet.from_io(pub).should be_a(MQTT::Protocol::PubAck)
          end

          # v3 framing carries no properties section; the packet must still be
          # well-formed with the right payload/topic (properties must not corrupt it).
          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          String.new(delivered.payload).should eq("hi")
          delivered.topic.should eq("test/topic")
          delivered.properties.user_properties.should be_empty
        end
      end
    end

    it "does not deliver a PUBLISH exceeding the subscriber's Maximum Packet Size" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          props = MQTT::Protocol::ConnectProperties.new
          props.maximum_packet_size = 50u32
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub", properties: props)
          subscribe(sub, topic_filters: [subtopic("t", 1)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            publish(pub, topic: "t", payload: Bytes.new(200, 0u8), qos: 1u8) # over 50 -> dropped
            publish(pub, topic: "t", payload: "ok".to_slice, qos: 1u8)       # under 50 -> delivered
          end

          # The oversized message is discarded [MQTT-3.1.2-25]; the subscriber
          # receives only the small one, and the big one is not redelivered.
          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          String.new(delivered.payload).should eq("ok")
          read_packet(sub).should be_nil
        end
      end
    end

    it "does not deliver an oversized QoS 0 PUBLISH exceeding the subscriber's Maximum Packet Size" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          props = MQTT::Protocol::ConnectProperties.new
          props.maximum_packet_size = 50u32
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub", properties: props)
          subscribe(sub, topic_filters: [subtopic("t", 0)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            publish(pub, topic: "t", payload: Bytes.new(200, 0u8), qos: 0u8) # over 50 -> dropped
            publish(pub, topic: "t", payload: "ok".to_slice, qos: 0u8)       # under 50 -> delivered
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          String.new(delivered.payload).should eq("ok")
        end
      end
    end

    it "keeps the Maximum Packet Size of a subscriber that connected without a client id" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          props = MQTT::Protocol::ConnectProperties.new
          # Room for the CONNACK, which echoes the assigned client id, but not
          # for the 200-byte payload.
          props.maximum_packet_size = 120u32
          # Empty client id: the server assigns one and rebuilds the CONNECT.
          # The rebuild must carry the properties over, or the limit is lost.
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "",
            clean_session: true, properties: props)
          subscribe(sub, topic_filters: [subtopic("t", 1)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            publish(pub, topic: "t", payload: Bytes.new(200, 0u8), qos: 1u8) # over 120 -> dropped
            publish(pub, topic: "t", payload: "ok".to_slice, qos: 1u8)       # under 120 -> delivered
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          String.new(delivered.payload).should eq("ok")
          read_packet(sub).should be_nil
        end
      end
    end

    it "delivers an oversized PUBLISH when the subscriber sets no Maximum Packet Size (v5)" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("t", 1)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            publish(pub, topic: "t", payload: Bytes.new(200, 7u8), qos: 1u8)
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          delivered.payload.size.should eq(200)
        end
      end
    end

    it "delivers a large PUBLISH to a v3 subscriber (no Maximum Packet Size in v3)" do
      with_server do |server|
        with_client_io(server) do |sub| # v3
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("t", 1)], packet_id: 1u16)

          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            publish(pub, topic: "t", payload: Bytes.new(200, 3u8), qos: 1u8)
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          delivered.payload.size.should eq(200)
        end
      end
    end

    it "answers a QoS 2 publish with PUBREC and keeps the connection open" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The CONNACK carries no Maximum QoS, which means 2 (3.2.2.3.4).
          MQTT::Protocol::Publish.new(
            topic: "test/topic", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 2u8, retain: false,
          ).to_io(io)
          io.flush

          pubrec = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubRec)
          pubrec.packet_id.should eq 1u16
          pubrec.reason_code.should eq MQTT::Protocol::PubRec::ReasonCode::NoMatchingSubscribers
        end
      end
    end

    it "disconnects with ProtocolError (0x82) on an empty topic with no alias" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The shard refuses to encode an empty-topic PUBLISH, so send raw bytes
          # for a v5 QoS 0 PUBLISH with an empty topic, empty properties, payload
          # "x": [0x30, remaining=4, topic-len=0x0000, props-len=0x00, 'x'].
          io.write_bytes_raw(Bytes[0x30, 0x04, 0x00, 0x00, 0x00, 0x78])
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "disconnects with ProtocolError (0x82) on a QoS 1 PUBLISH with packet id 0 [MQTT-2.2.1-3]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)
          publish(io, topic: "test/topic", qos: 1u8, packet_id: 0u16, expect_response: false)

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "delivers at the minimum of publish and subscription qos [MQTT-3.8.4-8]" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO.v5(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("test/topic", 1)], packet_id: 1u16)

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO.v5(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            publish(pub, topic: "test/topic", qos: 0u8)
            publish(pub, topic: "test/topic", qos: 1u8, packet_id: 2u16)
          end

          # The qos1 subscription must not upgrade the qos0 publish, so the first
          # delivery carries no packet id and is not ackable.
          first = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          first.qos.should eq(0u8)
          first.packet_id.should be_nil
          second = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          second.qos.should eq(1u8)
          second.packet_id.should_not be_nil
          puback(sub, second.packet_id)
        end
      end
    end
    it "delivers a message whose mqtt.* header is out of range instead of poisoning the queue" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)

          # An AMQP client can bind mqtt.<client-id> to amq.topic and publish
          # anything, so a header `store` would never write must not raise inside
          # build_packet - that requeues and re-raises, force-closing the
          # subscriber, which re-poisons on reconnect.
          headers = LavinMQ::AMQP::Table.new({
            "mqtt.message_expiry_interval" => -1_i32,
            "mqtt.response_topic"          => "reply/#",
          })
          props = LavinMQ::AMQP::Properties.new(headers: headers, delivery_mode: 1u8)
          body = "poison"
          msg = LavinMQ::Message.new(RoughTime.unix_ms, LavinMQ::MQTT::EXCHANGE, "a/b",
            props, body.bytesize.to_u64, ::IO::Memory.new(body))
          server.vhosts["/"].session("mqtt.sub").publish(msg)

          delivered = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          delivered.payload.should eq("poison".to_slice)
          delivered.properties.message_expiry_interval.should be_nil
          delivered.properties.response_topic.should be_nil
          puback(io, delivered.packet_id)
          pingpong(io)

          session = server.vhosts["/"].session("mqtt.sub")
          session.unacked_count.should eq 0
          session.message_count.should eq 0
        end
      end
    end
  end
end
