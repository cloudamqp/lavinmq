require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  private def self.v5_connect(socket, **args)
    io = MQTT::Protocol::IO::V5.new(socket)
    connect(io, **{version: MQTT::Protocol::Version::V5}.merge(args))
    io
  end

  describe "MQTT 5.0 subscription options" do
    describe "No Local [MQTT-3.8.3-3]" do
      it "does not deliver a message back to the client that published it" do
        with_server do |server|
          with_client_socket(server) do |socket|
            io = v5_connect(socket, client_id: "self")
            subscribe(io, topic_filters: [subtopic("a/b", 1u8, no_local: true)])
            # QoS 1 so the PUBACK proves the publish was fully handled before
            # we assert that nothing came back.
            publish(io, topic: "a/b", qos: 1u8)
            io.should be_drained
          end
        end
      end

      it "still delivers the message to other subscribers of the same filter" do
        with_server do |server|
          with_client_socket(server) do |other_socket|
            other = v5_connect(other_socket, client_id: "other")
            subscribe(other, topic_filters: [subtopic("a/b", 1u8)])

            with_client_socket(server) do |socket|
              io = v5_connect(socket, client_id: "self")
              subscribe(io, topic_filters: [subtopic("a/b", 1u8, no_local: true)])
              publish(io, topic: "a/b", qos: 1u8, payload: "hello".to_slice)
              io.should be_drained
            end

            delivered = read_packet(other).as(MQTT::Protocol::Publish)
            delivered.topic.should eq "a/b"
            String.new(delivered.payload).should eq "hello"
          end
        end
      end

      it "delivers a client its own message when No Local is not set" do
        # The guard against over-suppressing: this is the pre-existing
        # behaviour and it must survive.
        with_server do |server|
          with_client_socket(server) do |socket|
            io = v5_connect(socket, client_id: "self")
            subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
            publish(io, topic: "a/b", qos: 1u8, payload: "mine".to_slice)
            delivered = read_packet(io).as(MQTT::Protocol::Publish)
            String.new(delivered.payload).should eq "mine"
          end
        end
      end

      it "suppresses only the publisher when two clients both set No Local" do
        with_server do |server|
          with_client_socket(server) do |a_socket|
            a = v5_connect(a_socket, client_id: "a")
            subscribe(a, topic_filters: [subtopic("a/b", 1u8, no_local: true)])

            with_client_socket(server) do |b_socket|
              b = v5_connect(b_socket, client_id: "b")
              subscribe(b, topic_filters: [subtopic("a/b", 1u8, no_local: true)])

              publish(b, topic: "a/b", qos: 1u8, payload: "from-b".to_slice)
              b.should be_drained

              from_b = read_packet(a).as(MQTT::Protocol::Publish)
              String.new(from_b.payload).should eq "from-b"
              puback(a, from_b.packet_id)

              publish(a, topic: "a/b", qos: 1u8, payload: "from-a".to_slice)
              a.should be_drained

              from_a = read_packet(b).as(MQTT::Protocol::Publish)
              String.new(from_a.payload).should eq "from-a"
            end
          end
        end
      end

      it "answers NoMatchingSubscribers when the only subscriber was itself" do
        # A visible v5 consequence: PubAck 0x10 comes from the routed count,
        # which No Local can now legitimately drive to zero. The spec makes
        # this reason code a MAY, so it is legal - but assert it on purpose.
        with_server do |server|
          with_client_socket(server) do |socket|
            io = v5_connect(socket, client_id: "self")
            subscribe(io, topic_filters: [subtopic("a/b", 1u8, no_local: true)])
            publish(io, false, topic: "a/b", qos: 1u8, packet_id: 1u16)
            io.flush
            ack = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubAck)
            ack.reason_code.should eq MQTT::Protocol::PubAck::ReasonCode::NoMatchingSubscribers
          end
        end
      end
    end

    describe "Retain As Published" do
      it "keeps the publisher's retain flag when set" do
        with_server do |server|
          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_as_published: true)])

            with_client_socket(server) do |pub_socket|
              pub = v5_connect(pub_socket, client_id: "pub")
              publish(pub, topic: "a/b", qos: 1u8, retain: true)
            end

            read_packet(sub).as(MQTT::Protocol::Publish).retain?.should be_true
          end
        end
      end

      it "clears the retain flag when not set [MQTT-3.3.1-9]" do
        with_server do |server|
          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8)])

            with_client_socket(server) do |pub_socket|
              pub = v5_connect(pub_socket, client_id: "pub")
              publish(pub, topic: "a/b", qos: 1u8, retain: true)
            end

            read_packet(sub).as(MQTT::Protocol::Publish).retain?.should be_false
          end
        end
      end

      it "does not set the retain flag for a publish that was not retained" do
        with_server do |server|
          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_as_published: true)])

            with_client_socket(server) do |pub_socket|
              pub = v5_connect(pub_socket, client_id: "pub")
              publish(pub, topic: "a/b", qos: 1u8, retain: false)
            end

            read_packet(sub).as(MQTT::Protocol::Publish).retain?.should be_false
          end
        end
      end

      # The leak: the retain flag is varied per matched subscription inside one
      # walk of the subscription tree, so writing it only for the subscriptions
      # that asked for it would let a `true` bleed into every later subscriber.
      # Both orders, because only one of them catches that and the tree is
      # walked in an order these specs must not depend on.
      it "does not leak the retain flag to a later plain subscriber" do
        with_server do |server|
          with_client_socket(server) do |rap_socket|
            with_client_socket(server) do |plain_socket|
              rap = v5_connect(rap_socket, client_id: "rap")
              plain = v5_connect(plain_socket, client_id: "plain")
              subscribe(rap, topic_filters: [subtopic("a/b", 1u8, retain_as_published: true)])
              subscribe(plain, topic_filters: [subtopic("a/b", 1u8)])

              with_client_socket(server) do |pub_socket|
                pub = v5_connect(pub_socket, client_id: "pub")
                publish(pub, topic: "a/b", qos: 1u8, retain: true)
              end

              read_packet(rap).as(MQTT::Protocol::Publish).retain?.should be_true
              read_packet(plain).as(MQTT::Protocol::Publish).retain?.should be_false
            end
          end
        end
      end

      it "does not leak the retain flag to a later RAP subscriber" do
        with_server do |server|
          with_client_socket(server) do |plain_socket|
            with_client_socket(server) do |rap_socket|
              plain = v5_connect(plain_socket, client_id: "plain")
              rap = v5_connect(rap_socket, client_id: "rap")
              subscribe(plain, topic_filters: [subtopic("a/b", 1u8)])
              subscribe(rap, topic_filters: [subtopic("a/b", 1u8, retain_as_published: true)])

              with_client_socket(server) do |pub_socket|
                pub = v5_connect(pub_socket, client_id: "pub")
                publish(pub, topic: "a/b", qos: 1u8, retain: true)
              end

              read_packet(plain).as(MQTT::Protocol::Publish).retain?.should be_false
              read_packet(rap).as(MQTT::Protocol::Publish).retain?.should be_true
            end
          end
        end
      end

      it "still sends retained messages with retain=1 at subscribe time" do
        # Retain As Published governs forwarded messages only; a replay from the
        # retain store always carries retain=1 [MQTT-3.3.1-8].
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_as_published: false)])
            read_packet(sub).as(MQTT::Protocol::Publish).retain?.should be_true
          end
        end
      end
    end

    # Beware when checking these against MQTT-v5.0-spec.txt: the body text at
    # line 1502 governs; the Appendix B copy of [MQTT-3.3.1-10] at line 3278
    # states the value-1 case inverted. That is an OASIS erratum.
    describe "Retain Handling" do
      it "sends retained messages at subscribe when 0 [MQTT-3.3.1-9]" do
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 0)])
            read_packet(sub).as(MQTT::Protocol::Publish).topic.should eq "a/b"
          end
        end
      end

      it "never sends retained messages when 2 [MQTT-3.3.1-11]" do
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 2)])
            sub.should be_drained
            # Still nothing on a re-subscribe, new or not.
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 2)])
            sub.should be_drained
          end
        end
      end

      it "sends retained messages when 1 and the subscription is new [MQTT-3.3.1-10]" do
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 1)])
            read_packet(sub).as(MQTT::Protocol::Publish).topic.should eq "a/b"
          end
        end
      end

      it "does not send them again when 1 and the subscription exists [MQTT-3.3.1-10]" do
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 1)])
            first = read_packet(sub).as(MQTT::Protocol::Publish)
            puback(sub, first.packet_id)

            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 1)])
            sub.should be_drained
          end
        end
      end

      it "treats a re-subscribe with a different QoS as existing, not new" do
        # [MQTT-3.8.4-3] replaces a subscription whose topic filter is
        # identical, so a changed QoS is a replacement and not a new
        # subscription - the subtle case, since the binding arguments differ.
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "a/b", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("a/b", 0u8, retain_handling: 1)])
            read_packet(sub).as(MQTT::Protocol::Publish).topic.should eq "a/b"

            subscribe(sub, topic_filters: [subtopic("a/b", 1u8, retain_handling: 1)])
            sub.should be_drained
          end
        end
      end

      it "treats a filter equal to the session's own queue name as new" do
        # queue_bindings prepends a synthetic default-exchange binding whose
        # routing key is the queue name, so a client subscribing to
        # `mqtt.<its own client id>` used to look like an existing subscription.
        with_server do |server|
          with_client_socket(server) do |pub_socket|
            pub = v5_connect(pub_socket, client_id: "pub")
            publish(pub, topic: "mqtt.sub", qos: 1u8, retain: true)
          end

          with_client_socket(server) do |sub_socket|
            sub = v5_connect(sub_socket, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic("mqtt.sub", 1u8, retain_handling: 1)])
            read_packet(sub).as(MQTT::Protocol::Publish).topic.should eq "mqtt.sub"
          end
        end
      end
    end

    describe "persistence" do
      it "keeps the options of a durable session's subscription across a restart" do
        with_server do |server|
          with_client_socket(server) do |socket|
            io = MQTT::Protocol::IO::V5.new(socket)
            props = MQTT::Protocol::ConnectProperties.new
            props.session_expiry_interval = 3600u32
            connect(io, version: MQTT::Protocol::Version::V5,
              client_id: "sub", clean_session: false, properties: props)
            subscribe(io, topic_filters: [
              subtopic("a/b", 1u8, no_local: true, retain_as_published: true),
            ])
            disconnect(io)
          end

          restart_server(server)

          exchange = server.vhosts["/"].mqtt_exchange
          binding = exchange.bindings_details.first
          binding.binding_key.routing_key.should eq "a/b"
          options = binding.binding_key.as(LavinMQ::MQTT::SubscriptionKey).options
          options.qos.should eq 1u8
          options.no_local?.should be_true
          options.retain_as_published?.should be_true
        end
      end
    end

    describe "MQTT 3.1.1" do
      it "is unaffected: a v3 client still receives its own published messages" do
        # The v3 wire has no options byte at all - IO::V3 rejects a SUBSCRIBE
        # with any of bits 7-2 set - so the new code paths are structurally
        # unreachable from v3 rather than merely defaulted.
        with_server do |server|
          with_client_io(server) do |io|
            connect(io, client_id: "v3")
            subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
            publish(io, topic: "a/b", qos: 1u8, retain: true)
            delivered = read_packet(io).as(MQTT::Protocol::Publish)
            delivered.topic.should eq "a/b"
            # retain cleared on a forwarded message, retained replay untouched
            delivered.retain?.should be_false
          end
        end
      end
    end
  end
end
