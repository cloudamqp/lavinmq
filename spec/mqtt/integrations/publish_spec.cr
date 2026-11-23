require "../spec_helper"
require "log/spec"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # Reads a PUBLISH from the raw socket up to its packet id, leaving the
  # payload unread. Returns the packet id and the payload size.
  def self.read_publish_up_to_packet_id(socket : IO) : {UInt16, Int32}
    (socket.read_bytes(UInt8) >> 4).should eq 3 # PUBLISH
    remaining = 0
    shift = 0
    loop do
      byte = socket.read_bytes(UInt8)
      remaining |= (byte & 0x7f).to_i << shift
      shift += 7
      break if byte < 0x80
    end
    topic_size = socket.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    socket.skip(topic_size)
    packet_id = socket.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    {packet_id, remaining - 2 - topic_size - 2}
  end

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

    it "closes the connection on a publish with a topic longer than 255 bytes" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub", clean_session: false)
          subscribe(sub, topic_filters: [subtopic("#", 1u8)])

          with_client_io(server) do |io|
            connect(io, client_id: "pub")
            expect_raises(IO::Error) do
              publish(io, topic: "a/#{"x" * 300}", qos: 1u8)
            end
          end

          with_client_io(server) do |io|
            connect(io, client_id: "pub")
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

    it "logs a warning when closing a connection over a too long topic" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          Log.capture("lmq.mqtt.client", :warn) do |logs|
            expect_raises(IO::Error) do
              publish(io, topic: "a/#{"x" * 300}", qos: 1u8)
            end
            logs.check("a warning carrying the topic length") do |entry|
              ex = entry.exception
              entry.severity.warn? && ex.is_a?(LavinMQ::MQTT::TopicTooLong) &&
                ex.message == "topic is 302 bytes, max 255"
            end
          end
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

    it "accepts a PUBACK that arrives while the PUBLISH is still being sent" do
      with_server do |server|
        # Larger than the socket buffers, so the send blocks mid-payload
        payload = Bytes.new(32 * 1024 * 1024) { |i| (i % 251).to_u8 }
        with_client_io(server) do |sub|
          connect(sub, client_id: "fast")
          subscribe(sub, topic_filters: mk_topic_filters({"t", 1u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            publish(pub, topic: "t", payload: payload, qos: 1u8)
            disconnect(pub)
          end
          # Ack before reading the payload, while the server is still
          # writing it
          packet_id, payload_size = read_publish_up_to_packet_id(sub.io)
          puback(sub, packet_id)
          received = Bytes.new(payload_size)
          sub.io.read_fully(received)
          received.should eq payload
          # Still connected, and the message was acked
          pingpong(sub)
          server.vhosts["/"].session("mqtt.fast").@unacked.should be_empty
          disconnect(sub)
        end
      end
    end

    it "doesn't requeue a message acked while its send was still running when the send fails" do
      with_server do |server|
        # Larger than the socket buffers, so the send blocks mid-payload
        payload = Bytes.new(32 * 1024 * 1024) { |i| (i % 251).to_u8 }
        session = nil
        with_client_io(server) do |sub|
          connect(sub, client_id: "acked", clean_session: false)
          subscribe(sub, topic_filters: mk_topic_filters({"t", 1u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            publish(pub, topic: "t", payload: payload, qos: 1u8)
            disconnect(pub)
          end
          s = server.vhosts["/"].session("mqtt.acked")
          session = s
          packet_id, _ = read_publish_up_to_packet_id(sub.io)
          puback(sub, packet_id)
          wait_for { s.@unacked.empty? }
          # Gone without reading the payload, so the send fails
        end
        s = session.should_not be_nil
        wait_for { s.client.nil? }
        s.message_count.should eq 0
        s.unacked_count.should eq 0
      end
    end

    it "keeps a message whose PUBLISH can't be built" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "unbuildable", clean_session: false)
          subscribe(sub, topic_filters: mk_topic_filters({"a/#", 1u8}))
          session = server.vhosts["/"].session("mqtt.unbuildable")
          # A wildcard topic makes Protocol::Publish.new raise
          props = AMQ::Protocol::Properties.new(delivery_mode: 1u8)
          session.publish(LavinMQ::Message.new("mqtt.default", "a/+", "body", props))
          wait_for { session.client.nil? }
          session.message_count.should eq 1
        end
      end
    end

    it "routes to other subscribers and acks when a subscriber's session closes mid fan-out" do
      with_server do |server|
        with_client_io(server) do |early|
          connect(early, client_id: "early")
          subscribe(early, topic_filters: mk_topic_filters({"a/b", 1u8}))
          with_client_io(server) do |closing_io|
            connect(closing_io, client_id: "closing")
            subscribe(closing_io, topic_filters: mk_topic_filters({"a/b", 1u8}))
            closing = server.vhosts["/"].session("mqtt.closing")
            lock = closing.@msg_store_lock
            with_client_io(server) do |pub|
              connect(pub, client_id: "pub")
              lock.lock
              publish(pub, topic: "a/b", payload: "1".to_slice, qos: 1u8, expect_response: false)
              sleep 100.milliseconds # the publisher is parked on the store lock
              lock.unlock
              # Closed before the parked publisher runs, as nothing here yields
              closing.close
              read_publish(early).payload.should eq "1".to_slice
              # A failed fan-out closes the publisher without a PUBACK, and its
              # re-send is routed again to the subscribers that had it
              read_packet(pub).should be_a(MQTT::Protocol::PubAck)
              disconnect(pub)
            end
          end
          disconnect(early)
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

    it "disconnects a publisher and doesn't route its message when the server is low on disk space" do
      # Keeps the stats loop from starting flow again
      LavinMQ::Config.instance.free_disk_min = Int64::MAX
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: mk_topic_filters({"t", 1u8}))
          server.flow(false)
          Log.capture("lmq.*", :error) do |logs|
            with_client_io(server) do |pub|
              connect(pub, client_id: "pub")
              publish(pub, topic: "t", payload: "m".to_slice, qos: 1u8, expect_response: false)
              pub.should be_closed
            end
            # The read loop has ended once the connection is removed
            wait_for { server.vhosts["/"].connections_size == 1 }
            # An intentional close, not "Client unexpectedly closed connection"
            logs.empty
          end
          sub.should be_drained
        end
      end
    ensure
      LavinMQ::Config.instance.free_disk_min = 0
    end
  end
end
