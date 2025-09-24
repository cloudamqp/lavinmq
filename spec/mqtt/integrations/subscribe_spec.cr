require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "subscribe" do
    it "pub/sub" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")

          topic_filters = mk_topic_filters({"test", 0})
          subscribe(sub_io, topic_filters: topic_filters)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")

            payload = Bytes[1, 254, 200, 197, 123, 4, 87]
            packet_id = next_packet_id
            ack = publish(pub_io,
              topic: "test",
              payload: payload,
              qos: 0u8,
              packet_id: packet_id
            )
            ack.should be_nil

            msg = read_packet(sub_io)
            msg.should be_a(MQTT::Protocol::Publish)
            msg = msg.as(MQTT::Protocol::Publish)
            msg.payload.should eq payload
            msg.packet_id.should be_nil # QoS=0
          end
        end
      end
    end

    it "bits 3,2,1,0 must be set to 0,0,1,0 [MQTT-3.8.1-1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          temp_io = IO::Memory.new
          topic_filters = mk_topic_filters({"a/b", 0})
          subscribe(MQTT::Protocol::IO.new(temp_io), topic_filters: topic_filters, expect_response: false)
          temp_io.rewind
          subscribe_pkt = temp_io.to_slice
          # This will overwrite the protocol level byte
          subscribe_pkt[0] |= 0b0000_1010u8
          io.write_bytes_raw subscribe_pkt

          # Verify that connection is closed
          io.should be_closed
        end
      end
    end

    it "must contain at least one topic filter [MQTT-3.8.3-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          topic_filters = mk_topic_filters({"a/b", 0})
          temp_io = IO::Memory.new
          subscribe(MQTT::Protocol::IO.new(temp_io), topic_filters: topic_filters, expect_response: false)
          temp_io.rewind
          sub_pkt = temp_io.to_slice
          sub_pkt[1] = 2u8 # Override remaning length
          io.write_bytes_raw sub_pkt

          # Verify that connection is closed
          io.should be_closed
        end
      end
    end

    it "should not allow any payload reserved bits to be set [MQTT-3-8.3-4]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          topic_filters = mk_topic_filters({"a/b", 0})
          temp_io = IO::Memory.new
          subscribe(MQTT::Protocol::IO.new(temp_io), topic_filters: topic_filters, expect_response: false)
          temp_io.rewind
          sub_pkt = temp_io.to_slice
          sub_pkt[sub_pkt.size - 1] |= 0b1010_0100u8
          io.write_bytes_raw sub_pkt

          # Verify that connection is closed
          io.should be_closed
        end
      end
    end

    it "should replace old subscription with new [MQTT-3.8.4-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          topic_filters = mk_topic_filters({"a/b", 0})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          # Verify that we subscribed as qos0
          suback.return_codes.first.should eq(MQTT::Protocol::SubAck::ReturnCode::QoS0)

          # Publish something to the topic we're subscribed to...
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8)
          # ... consume it...
          packet = read_packet(io).as(MQTT::Protocol::Publish)
          # ... and verify it be qos0 (i.e. our subscribe is correct)
          packet.qos.should eq(0u8)

          # Now do a second subscribe with another qos and do the same verification
          topic_filters = mk_topic_filters({"a/b", 1})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          # Verify that we subscribed as qos1
          suback.return_codes.should eq([MQTT::Protocol::SubAck::ReturnCode::QoS1])

          # Publish something to the topic we're subscribed to...
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8)
          # ... consume it...
          packet = read_packet(io).as(MQTT::Protocol::Publish)
          # ... and verify it be qos1 (i.e. our second subscribe is correct)
          packet.qos.should eq(1u8)

          io.should be_drained
        end
      end
    end
  end
end
