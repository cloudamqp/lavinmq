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

    it "keeps delivering to a session after a publish with a topic longer than 255 bytes" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub", clean_session: false)
          subscribe(sub, topic_filters: [subtopic("#", 1u8)])

          with_client_io(server) do |io|
            connect(io, client_id: "pub")
            publish(io, topic: "a/#{"x" * 300}", qos: 1u8).should be_a(MQTT::Protocol::PubAck)
            publish(io, topic: "a/b", qos: 1u8).should be_a(MQTT::Protocol::PubAck)
            disconnect(io)
          end

          pub = read_publish(sub)
          pub.topic.should eq "a/b"
          puback(sub, pub.packet_id)
          server.vhosts["/"].session("mqtt.sub").closed?.should be_false
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

    it "reads a PUBLISH that arrives in parts" do
      with_server do |server|
        payload = Bytes.new(1000) { |i| (i % 251).to_u8 }
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: mk_topic_filters({"a/b", 0u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            packet = publish_packet(topic: "a/b", payload: payload, qos: 1u8)
            buf = IO::Memory.new
            packet.to_io(MQTT::Protocol::IO.new(buf))
            bytes = buf.to_slice
            # Split after the type byte, in the remaining length, in the topic
            # and in the payload, with a pause so the server reads each part
            # before the next arrives. Without Nagle, so the kernel sends each
            # part when it's written.
            pub.io.as(TCPSocket).tcp_nodelay = true
            {0, 1, 2, 6, 500, bytes.size}.each_cons_pair do |from, to|
              pub.io.write bytes[from, to - from]
              sleep 10.milliseconds
            end
            ack = read_packet(pub).should be_a(MQTT::Protocol::PubAck)
            ack.packet_id.should eq packet.packet_id
          end
          msg = read_publish(sub)
          msg.topic.should eq "a/b"
          msg.payload.should eq payload
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

    it "doesn't resend buffered data when disconnecting a subscriber that doesn't read" do
      LavinMQ::Config.instance.tcp_send_timeout = 1
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "stuck")
          subscribe(sub, topic_filters: mk_topic_filters({"t", 0u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            payload = Bytes.new(1000)
            # more than the socket buffers can hold, so delivery to the
            # subscriber blocks
            20_000.times { publish(pub, topic: "t", payload: payload, qos: 0u8) }
            disconnect(pub)
          end
          session = server.vhosts["/"].session("mqtt.stuck")
          wait_for { session.message_count > 0 }
          sleep 1.5.seconds # the first write has timed out, the client is closing
          sub.io.as(TCPSocket).read_timeout = 10.seconds
          packets = 0
          loop do
            # raises on a duplicated, so misaligned, packet
            read_packet(sub).as(MQTT::Protocol::Publish)
            packets += 1
          rescue IO::EOFError
            break
          end
          packets.should be > 0
        end
      end
    end
  end
end
