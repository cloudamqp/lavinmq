require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "qos2 as receiver" do
    it "completes the QoS 2 handshake for an inbound publish [MQTT-4.3.3-1]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher")
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
            pubrel(io, 7u16)
            read_packet(io).should be_a(MQTT::Protocol::PubComp)
            disconnect(io)
          end

          pub = read_publish(sub_io)
          String.new(pub.payload).should eq "1"
          read_packet(sub_io).should be_nil

          disconnect(sub_io)
        end
      end
    end

    it "delivers a re-sent QoS 2 publish only once [MQTT-4.3.3-1]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          # QoS 0 subscription, so the outbound handshake plays no part here.
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher")
            # The same packet id twice, never released by a PUBREL. Both are
            # answered with PUBREC, only the first is delivered onward.
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          read_packet(sub_io).should be_nil

          disconnect(sub_io)
        end
      end
    end

    it "treats a repeat of a released packet id as a new message" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher")
            publish_qos2(io, 7u16, topic: "a/b", payload: "1".to_slice)
            # The PUBREL above released id 7, so it is free to name a second
            # message rather than being deduped against the first.
            publish_qos2(io, 7u16, topic: "a/b", payload: "2".to_slice)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          String.new(read_publish(sub_io).payload).should eq "2"

          disconnect(sub_io)
        end
      end
    end

    it "answers PUBCOMP to a PUBREL for an unknown packet id" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          pubrel(io, 99u16)
          read_packet(io).should be_a(MQTT::Protocol::PubComp)
          # Still usable: an unknown id must not be treated as a protocol error,
          # since nothing about the held ids survives a broker restart.
          io.should be_drained

          disconnect(io)
        end
      end
    end

    it "keeps inbound QoS 2 state across a persistent reconnect [MQTT-4.4.0-1]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          # A persistent session that subscribes, so it survives the disconnect
          # below and can still be resumed.
          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            subscribe(io, topic_filters: mk_topic_filters({"unused", 0u8}))
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
            # Gone without releasing the id.
          end

          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
            pubrel(io, 7u16)
            read_packet(io).should be_a(MQTT::Protocol::PubComp)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          read_packet(sub_io).should be_nil

          disconnect(sub_io)
        end
      end
    end

    it "drops inbound QoS 2 state on a clean session [MQTT-3.1.2-6]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            subscribe(io, topic_filters: mk_topic_filters({"unused", 0u8}))
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          end

          # A clean CONNECT discards the held id, so the re-send is a new message
          # and is delivered a second time. The mirror of the spec above.
          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: true)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
            pubrel(io, 7u16)
            read_packet(io).should be_a(MQTT::Protocol::PubComp)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          String.new(read_publish(sub_io).payload).should eq "1"

          disconnect(sub_io)
        end
      end
    end
  end
end
