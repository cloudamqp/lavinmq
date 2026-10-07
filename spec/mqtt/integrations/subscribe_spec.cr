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
          subscribe(MQTT::Protocol::IO.v3(temp_io), topic_filters: topic_filters, expect_response: false)
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

    it "must contain at least one topic filter [MQTT-3.8.3-2]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          topic_filters = mk_topic_filters({"a/b", 0})
          temp_io = IO::Memory.new
          subscribe(MQTT::Protocol::IO.v3(temp_io), topic_filters: topic_filters, expect_response: false)
          temp_io.rewind
          sub_pkt = temp_io.to_slice
          sub_pkt[1] = 2u8 # Override remaning length
          io.write_bytes_raw sub_pkt

          # Verify that connection is closed
          io.should be_closed
        end
      end
    end

    it "should not allow any payload reserved bits to be set [MQTT-3.8.3-4 v3.1.1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          topic_filters = mk_topic_filters({"a/b", 0})
          temp_io = IO::Memory.new
          subscribe(MQTT::Protocol::IO.v3(temp_io), topic_filters: topic_filters, expect_response: false)
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
          suback.reason_codes.first.should eq(MQTT::Protocol::SubAck::ReasonCode::GrantedQos0)

          # Publish something to the topic we're subscribed to...
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8, expect_response: false)
          # ... consume it...
          packet = read_delivery_and_puback(io)
          # ... and verify it be qos0 (i.e. our subscribe is correct)
          packet.qos.should eq(0u8)

          # Now do a second subscribe with another qos and do the same verification
          topic_filters = mk_topic_filters({"a/b", 1})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          # Verify that we subscribed as qos1
          suback.reason_codes.should eq([MQTT::Protocol::SubAck::ReasonCode::GrantedQos1])

          # Publish something to the topic we're subscribed to...
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8, expect_response: false)
          # ... consume it...
          packet = read_delivery_and_puback(io)
          # ... and verify it be qos1 (i.e. our second subscribe is correct)
          packet.qos.should eq(1u8)

          io.should be_drained
        end
      end
    end

    it "grants qos2 for a qos2 subscription [MQTT-3.8.4-7]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          # The SubAck return code must show the granted maximum qos.
          topic_filters = mk_topic_filters({"a/b", 2})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq([MQTT::Protocol::SubAck::ReasonCode::GrantedQos2])

          # Published at qos 2 from a second connection, so that the grant is
          # what decides the delivery qos rather than the publish capping it
          # [MQTT-3.8.4-8], and so the publisher's PUBREC does not interleave
          # with the delivery on this socket.
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16)
            pubrel(pub_io, 1u16)
            read_packet(pub_io).should be_a(MQTT::Protocol::PubComp)
            disconnect(pub_io)
          end

          # The grant is real and not just reported in the SubAck.
          packet = read_publish(io)
          packet.qos.should eq(2u8)
          id = packet.packet_id.should_not be_nil

          pubrec(io, id)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          pubcomp(io, id)

          io.should be_drained
        end
      end
    end

    it "rebinds when a resubscribe changes the granted qos" do
      with_server do |server|
        exchange = server.vhosts["/"].exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        with_client_io(server) do |io|
          connect(io, client_id: "sub")
          session = client_session(server, "sub")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1}, {"c/#", 1}))
          session.subscriptions.transform_values(&.qos).should eq({"a/b" => 1u8, "c/#" => 1u8})
          exchange.bindings_details.find!(&.binding_key.routing_key.==("a/b")).binding_key.qos.should eq 1u8

          # The unbind-and-rebind path. Unreachable for 1 -> 2 until QoS 2
          # stopped collapsing onto QOS1_ARGUMENTS, which made the arguments
          # compare equal and take the early return instead.
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2}))
          exchange.bindings_details.size.should eq 2
          exchange.bindings_details.find!(&.binding_key.routing_key.==("a/b")).binding_key.qos.should eq 2u8
          session.subscriptions.transform_values(&.qos).should eq({"a/b" => 2u8, "c/#" => 1u8})
          session.bindings.compact_map(&.as?(LavinMQ::MQTT::SubscriptionDetails)).map { |b| {b.routing_key, LavinMQ::MQTT.subscription_options(b.arguments).qos} }
            .sort!.should eq [{"a/b", 2u8}, {"c/#", 1u8}]
          exchange.binding_count.should eq 2

          # And back down again.
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1}))
          exchange.bindings_details.size.should eq 2
          exchange.bindings_details.find!(&.binding_key.routing_key.==("a/b")).binding_key.qos.should eq 1u8

          unsubscribe(io, topics: ["c/#"])
          session.subscriptions.transform_values(&.qos).should eq({"a/b" => 1u8})
          disconnect(io)
        end
      end
    end

    it "binds a qos2 subscription as qos2" do
      with_server do |server|
        exchange = server.vhosts["/"].exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        with_client_io(server) do |io|
          connect(io)
          suback = subscribe(io, topic_filters: mk_topic_filters({"a/b", 2})).as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq [MQTT::Protocol::SubAck::ReasonCode::GrantedQos2]

          binding = exchange.bindings_details.first
          binding.binding_key.qos.should eq 2u8
          binding.arguments.should eq LavinMQ::MQTT::QOS2_ARGUMENTS

          # A follow up subscribe with the qos we granted is the same
          # subscription, so it must not add a second binding
          suback = subscribe(io, topic_filters: mk_topic_filters({"a/b", 2})).as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq [MQTT::Protocol::SubAck::ReasonCode::GrantedQos2]
          exchange.bindings_details.size.should eq 1
          exchange.binding_count.should eq 1

          disconnect(io)
        end
      end
    end

    it "grants Failure and keeps the connection when the session store cannot be created" do
      with_server do |server|
        vhost = server.vhosts["/"]
        # A file where the session's directory goes makes creating it fail
        File.write(File.join(vhost.data_dir, Digest::SHA1.hexdigest("mqtt.sub")), "")
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8})).as(MQTT::Protocol::SubAck)
          ack.reason_codes.should eq [MQTT::Protocol::SubAck::ReasonCode::UnspecifiedError]
          pingpong(io).should be_a(MQTT::Protocol::PingResp)
          disconnect(io)
        end
      end
    end

    it "grants Failure for a subscription whose session was deleted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("mqtt.gone", true, false, LavinMQ::MQTT::Session::ARGUMENTS)
        session = vhost.session("mqtt.gone")
        session.delete
        vhost.session?("mqtt.gone").should be_nil

        broker = server.mqtt_server.broker("/")
        [0u8, 1u8, 2u8].each do |qos|
          broker.grant(session, subtopic("a/b", qos))
            .should eq MQTT::Protocol::SubAck::ReasonCode::UnspecifiedError
        end
        vhost.mqtt_exchange.binding_count.should eq 0
      end
    end

    it "keeps the subscriptions of a persistent session after a restart" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: [subtopic("a/b", 0u8)])
          disconnect(io)
        end

        restart_server(server)

        vhost = server.vhosts["/"]
        vhost.session?("mqtt.sub").should_not be_nil
        exchange = vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        exchange.bindings_details.map(&.binding_key.routing_key).should eq ["a/b"]
      end
    end

    it "does not restore an unsubscribed topic after a restart" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: [subtopic("a/b", 0u8)])
          unsubscribe(io, ["a/b"])
          disconnect(io)
        end

        restart_server(server)

        server.vhosts["/"].mqtt_exchange.bindings_details.should be_empty
      end
    end
  end

  describe "a session's subscriptions" do
    it "follow an unbind that didn't come from the client" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          server.vhosts["/"].unbind_queue("mqtt.sub", LavinMQ::MQTT::EXCHANGE, "a/b", LavinMQ::MQTT::QOS1_ARGUMENTS)
          server.vhosts["/"].session("mqtt.sub").subscriptions.should be_empty
          # Subscribing again binds it again, rather than taking it as bound
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          server.vhosts["/"].mqtt_exchange.binding_count.should eq 1
          disconnect(io)
        end
      end
    end

    it "are restored at startup" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          disconnect(io)
        end
        restart_server(server)
        session = server.vhosts["/"].session("mqtt.sub")
        session.subscriptions.transform_values(&.qos).should eq({"a/b" => 1u8})
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: false)
          unsubscribe(io, topics: ["a/b"])
          session.subscriptions.should be_empty
          server.vhosts["/"].mqtt_exchange.binding_count.should eq 0
          disconnect(io)
        end
      end
    end

    it "are the only ones a deleted session takes along" do
      with_server do |server|
        with_client_io(server) do |other|
          connect(other, client_id: "other")
          subscribe(other, topic_filters: mk_topic_filters({"a/b", 0u8}))
          with_client_io(server) do |io|
            connect(io, client_id: "clean", clean_session: true)
            subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}, {"c/d", 0u8}))
            server.vhosts["/"].mqtt_exchange.binding_count.should eq 3
            disconnect(io)
          end
          wait_for { server.vhosts["/"].session?("mqtt.clean").nil? }
          server.vhosts["/"].mqtt_exchange.binding_count.should eq 1
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            publish(pub, topic: "a/b", payload: "still".to_slice, qos: 0u8)
            disconnect(pub)
          end
          String.new(read_publish(other).payload).should eq "still"
          disconnect(other)
        end
      end
    end
  end

  describe "MQTT 5.0 subscribe" do
    it "disconnects with ProtocolError (0x82) on Retain Handling 3 (§3.8.3.1)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The shard cannot encode Retain Handling 3, so send raw bytes: packet
          # id 1, empty properties, filter "a", options 0x30.
          io.write_bytes_raw(Bytes[0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 0x61, 0x30])
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "closes rather than send a SUBACK over the client's Maximum Packet Size [MQTT-3.1.2-24]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          props = MQTT::Protocol::ConnectProperties.new
          props.maximum_packet_size = 30u32
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "sub",
            properties: props).should be_a(MQTT::Protocol::Connack)
          # One reason code per filter, so 30 filters make a 35-byte SUBACK.
          filters = (1..30).map { |i| subtopic("t/#{i}", 0) }
          subscribe(io, topic_filters: filters, packet_id: 1u16, expect_response: false)
          io.should be_closed
        end
      end
    end

    it "grants a QoS 2 subscription as QoS 2" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The SUBACK reports the granted maximum [MQTT-3.8.4-7].
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 2u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16).to_io(io)
          io.flush

          suback = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq([MQTT::Protocol::SubAck::ReasonCode::GrantedQos2])
        end
      end
    end
  end
end
