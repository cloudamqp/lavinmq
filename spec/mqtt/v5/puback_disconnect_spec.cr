require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  private def self.v5_connect(socket, **args)
    io = MQTT::Protocol::IO::V5.new(socket)
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

  describe "MQTT 5.0 PUBACK" do
    it "answers NoMatchingSubscribers when nothing is subscribed [MQTT-3.4.2.1]" do
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
          io = MQTT::Protocol::IO::V3.new(socket)
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
            publish(pub, topic: "a/b", qos: 0u8)
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
end
