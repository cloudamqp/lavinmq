require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "unsubscribe" do
    it "bits 3,2,1,0 must be set to 0,0,1,0 [MQTT-3.10.1-1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          temp_io = IO::Memory.new
          unsubscribe(MQTT::Protocol::IO.new(temp_io), topics: ["a/b"], expect_response: false)
          temp_io.rewind
          unsubscribe_pkt = temp_io.to_slice
          # This will overwrite the protocol level byte
          unsubscribe_pkt[0] |= 0b0000_1010u8
          io.write_bytes_raw unsubscribe_pkt

          io.should be_closed
        end
      end
    end

    it "must contain at least one topic filter [MQTT-3.10.3-2]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          temp_io = IO::Memory.new
          unsubscribe(MQTT::Protocol::IO.new(temp_io), topics: ["a/b"], expect_response: false)
          temp_io.rewind
          unsubscribe_pkt = temp_io.to_slice
          # Overwrite remaining length
          unsubscribe_pkt[1] = 2u8
          io.write_bytes_raw unsubscribe_pkt

          io.should be_closed
        end
      end
    end

    it "must stop adding any new messages for delivery to the Client, but completes delivery of previous messages [MQTT-3.10.4-2] and [MQTT-3.10.4-3]" do
      with_server do |server|
        with_client_io(server) do |pubio|
          connect(pubio, client_id: "publisher")

          # Create a non-clean session with an active subscription
          with_client_io(server) do |io|
            connect(io, clean_session: false)
            topics = mk_topic_filters({"a/b", 1})
            subscribe(io, topic_filters: topics)
            disconnect(io)
          end
          pp "first consumers: #{server.vhosts["/"].queues["amq.mqtt-client_id"].consumers}"

          # Publish messages that will be stored for the subscriber
          2.times { |i| publish(pubio, topic: "a/b", payload: i.to_s.to_slice, qos: 0u8) }

          pp "first msg count: #{server.vhosts["/"].queues["amq.mqtt-client_id"].message_count}"

          # Let the subscriber connect and read the messages, but don't ack. Then unsubscribe.
          # We must read the Publish packets before unsubscribe, else the "suback" will be stuck.
          with_client_io(server) do |io|
            connect(io, clean_session: false)
            2.times do
              pkt = read_packet(io)
              pkt.should be_a(MQTT::Protocol::Publish)
              # dont ack
            end

            unsubscribe(io, topics: ["a/b"])
            disconnect(io)
          end
          pp "second msg count: #{server.vhosts["/"].queues["amq.mqtt-client_id"].message_count}"
          pp "unacked msgs: #{server.vhosts["/"].queues["amq.mqtt-client_id"].unacked_count}"

          # Publish more messages
          2.times { |i| publish(pubio, topic: "a/b", payload: (2 + i).to_s.to_slice, qos: 0u8) }

          # Now, if unsubscribed worked, the last two publish packets shouldn't be held for the
          # session. Read the two we expect, then test that there is nothing more to read.
          with_client_io(server) do |io|
            connect(io, clean_session: false)
            2.times do |i|
              pkt = read_packet(io)
              pkt.should be_a(MQTT::Protocol::Publish)
              pkt = pkt.as(MQTT::Protocol::Publish)
              pkt.payload.should eq(i.to_s.to_slice)
            end

            io.should be_drained
            disconnect(io)
          end
          disconnect(pubio)
        end
      end
    end
  end
end
