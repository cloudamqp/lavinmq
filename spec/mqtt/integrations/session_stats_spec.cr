require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT::Session stats" do
    it "increments publish_count when a message is published to the session" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            pingpong(pub_io)
            disconnect(pub_io)
          end

          pingpong(io)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.publish_count.should eq 1
          disconnect(io)
        end
      end
    end

    it "increments deliver_no_ack_count and deliver_get_count for QoS 0 deliveries" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            pingpong(pub_io)
            disconnect(pub_io)
          end

          read_packet(io)
          pingpong(io)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.deliver_no_ack_count.should eq 1
          session.deliver_get_count.should eq 1
          session.deliver_count.should eq 0
          session.redeliver_count.should eq 0
          disconnect(io)
        end
      end
    end

    it "increments deliver_count and deliver_get_count for QoS 1 deliveries" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            disconnect(pub_io)
          end

          pub = read_packet(io).as(MQTT::Protocol::Publish)
          puback(io, pub.packet_id)
          pingpong(io)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.deliver_count.should eq 1
          session.deliver_get_count.should eq 1
          session.deliver_no_ack_count.should eq 0
          session.redeliver_count.should eq 0
          disconnect(io)
        end
      end
    end

    it "tracks unacked_count and unacked_bytesize while QoS 1 message is in flight" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            disconnect(pub_io)
          end

          pub = read_packet(io).as(MQTT::Protocol::Publish)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.unacked_count.should eq 1
          session.unacked_bytesize.should be > 0

          puback(io, pub.packet_id)
          pingpong(io)

          session.unacked_count.should eq 0
          session.unacked_bytesize.should eq 0
          disconnect(io)
        end
      end
    end

    it "increments ack_count when a QoS 1 message is acknowledged" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            disconnect(pub_io)
          end

          pub = read_packet(io).as(MQTT::Protocol::Publish)
          puback(io, pub.packet_id)
          pingpong(io)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.ack_count.should eq 1
          disconnect(io)
        end
      end
    end

    it "increments redeliver_count for messages re-sent after reconnect" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 0u8)
            disconnect(pub_io)
          end

          read_packet(io)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, clean_session: false)
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.dup?.should be_true
          puback(io, pub.packet_id)
          pingpong(io)

          session = server.vhosts["/"].sessions["mqtt.client_id"]
          session.redeliver_count.should eq 1
          session.deliver_count.should eq 1
          session.deliver_no_ack_count.should eq 0
          disconnect(io)
        end
      end
    end
  end
end
