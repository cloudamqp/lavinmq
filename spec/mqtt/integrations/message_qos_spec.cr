require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "message qos" do
    it "both qos bits can't be set [MQTT-3.3.1-4]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          temp_io = IO::Memory.new
          publish(MQTT::Protocol::IO.new(temp_io), topic: "a/b", qos: 1u8, expect_response: false)
          pub_pkt = temp_io.to_slice
          pub_pkt[0] |= 0b0000_0110u8
          io.write pub_pkt

          io.should be_closed
        end
      end
    end

    it "qos is set according to subscription qos [LavinMQ non-normative]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          # Subscribe with qos=0 means downgrade messages to qos=0
          topic_filters = mk_topic_filters({"a/b", 0u8})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", qos: 0u8)
            publish(publisher_io, topic: "a/b", qos: 1u8)
            disconnect(publisher_io)
          end

          pub1 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub1.qos.should eq(0u8)
          pub1.dup?.should be_false
          pub2 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub2.qos.should eq(0u8)
          pub2.dup?.should be_false

          disconnect(io)
        end
      end
    end

    it "qos1 messages are stored for offline sessions [MQTT-3.1.2-5]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)
          disconnect(io)
        end

        with_client_io(server) do |publisher_io|
          connect(publisher_io, client_id: "publisher")
          100.times do
            # qos doesnt matter here
            publish(publisher_io, topic: "a/b", qos: 0u8)
          end
          disconnect(publisher_io)
        end

        with_client_io(server) do |io|
          connect(io)
          100.times do
            pkt = read_packet(io)
            pkt.should be_a(MQTT::Protocol::Publish)
            if pub = pkt.as?(MQTT::Protocol::Publish)
              puback(io, pub.packet_id)
            end
          end
          disconnect(io)
        end
      end
    end

    it "acked qos1 message won't be sent again" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", payload: "1".to_slice, qos: 0u8)
            publish(publisher_io, topic: "a/b", payload: "2".to_slice, qos: 0u8)
            disconnect(publisher_io)
          end

          pkt = read_packet(io)
          if pub = pkt.as?(MQTT::Protocol::Publish)
            pub.payload.should eq("1".to_slice)
            puback(io, pub.packet_id)
          end
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io)
          pkt = read_packet(io)
          if pub = pkt.as?(MQTT::Protocol::Publish)
            pub.payload.should eq("2".to_slice)
            puback(io, pub.packet_id)
          end
          disconnect(io)
        end
      end
    end

    it "acks must not be ordered" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            10.times do |i|
              publish(publisher_io, topic: "a/b", payload: "#{i}".to_slice, qos: 0u8)
            end
            disconnect(publisher_io)
          end

          pubs = Array(MQTT::Protocol::Publish).new(9)
          # Read all but one
          9.times do
            pubs << read_packet(io).as(MQTT::Protocol::Publish)
          end
          [1, 3, 4, 0, 2, 7, 5, 6, 8].each do |i|
            puback(io, pubs[i].packet_id)
          end
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io)
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.dup?.should be_true
          pub.payload.should eq("9".to_slice)
          disconnect(io)
        end
      end
    end

    it "cannot ack invalid packet id" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          # we need to subscribe in order to have a session
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)
          puback(io, 123u16)

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end

    it "cannot ack a message twice" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", qos: 0u8)
            disconnect(publisher_io)
          end

          pub = read_packet(io).as(MQTT::Protocol::Publish)

          puback(io, pub.packet_id)

          # Sending the second ack make the server close the connection
          puback(io, pub.packet_id)

          io.should be_closed
        end
      end
    end

    it "qos1 unacked messages re-sent in the initial order [MQTT-4.6.0-1]" do
      max_inflight_messages = 10
      # We'll only ACK odd packet ids, and the first id is 1, so if we don't
      # do -1 the last packet (id=20) won't be sent because we've reached max
      # inflight with all odd ids.
      number_of_messages = (max_inflight_messages * 2 - 1).to_u16
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          topic_filters = mk_topic_filters({"a/b", 1u8})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            number_of_messages.times do |i|
              data = Bytes.new(sizeof(UInt16))
              IO::ByteFormat::SystemEndian.encode(i, data)
              # qos doesnt matter here
              publish(publisher_io, topic: "a/b", payload: data, qos: 0u8)
            end
            disconnect(publisher_io)
          end

          # Read all messages, but only ack every second
          # sync = Spectator::Synchronizer.new
          sync = Channel(Bool).new(1)
          spawn(name: "read msgs") do
            number_of_messages.times do |i|
              pkt = read_packet(io)
              pub = pkt.should be_a(MQTT::Protocol::Publish)
              # We only ack odd packet ids
              puback(io, pub.packet_id) if (i % 2) > 0
            end
            sync.send true
            # sync.done
          end
          select
          when sync.receive
          when timeout(3.seconds)
            fail "Timeout first read"
          end
          # sync.synchronize(timeout: 3.second, msg: "Timeout first read")
          disconnect(io)
        end

        # We should now get the 50 messages we didn't ack previously, and in order
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          # sync = Spectator::Synchronizer.new
          sync = Channel(Bool).new(1)
          spawn(name: "read msgs") do
            (number_of_messages // 2).times do |i|
              pkt = read_packet(io)
              pkt.should be_a(MQTT::Protocol::Publish)
              pub = pkt.as(MQTT::Protocol::Publish)
              puback(io, pub.packet_id)
              data = IO::ByteFormat::SystemEndian.decode(UInt16, pub.payload)
              data.should eq(i * 2)
            end
            sync.send true
            # sync.done
          end
          select
          when sync.receive
          when timeout(3.seconds)
            puts "Timeout second read"
          end
          # sync.synchronize(timeout: 3.second, msg: "Timeout second read")
          disconnect(io)
        end
      end
    end
  end
end
