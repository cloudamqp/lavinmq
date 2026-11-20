require "../spec_helper"

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
  end
end
