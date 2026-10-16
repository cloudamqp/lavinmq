require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # Publishes two QoS 2 messages through the full receiver-side handshake from a
  # throwaway connection, so the client under test is only ever the subscriber.
  def self.publish_two_qos2(server, topic)
    with_client_io(server) do |pub_io|
      connect(pub_io, client_id: "publisher")
      2.times do |i|
        id = (i + 1).to_u16
        publish(pub_io, topic: topic, payload: i.to_s.to_slice, qos: 2u8, packet_id: id)
        pubrel(pub_io, id)
        read_packet(pub_io).should be_a(MQTT::Protocol::PubComp)
      end
      disconnect(pub_io)
    end
  end

  describe "qos2 as receiver" do
    it "delivers a re-sent QoS 2 publish only once [MQTT-4.3.3-2]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          # QoS 0 subscription, so the outbound handshake plays no part here.
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher")
            # Same id twice, never released: both get a PUBREC, one is routed.
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          sub_io.should be_silent

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

    it "closes a publisher that sends packet id 0 [MQTT-2.3.1-1]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          {1u8, 2u8}.each do |qos|
            with_client_io(server) do |io|
              connect(io, client_id: "publisher")
              publish(io, topic: "a/b", payload: "1".to_slice, qos: qos,
                packet_id: 0u16, expect_response: false)
              io.should be_closed
            end
          end

          # Neither was routed.
          sub_io.should be_silent

          disconnect(sub_io)
        end
      end
    end

    it "lets a publisher pipeline more QoS 2 publishes than the outbound window" do
      # `max_inflight_messages` is the server's outbound window and says nothing
      # about how many ids a publisher may hold.
      LavinMQ::Config.instance.max_inflight_messages = 1u16
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher")
            # Three unreleased ids at once, three times the window.
            3.times do |i|
              publish(io, topic: "a/b", payload: i.to_s.to_slice, qos: 2u8,
                packet_id: (i + 1).to_u16, expect_response: false)
            end
            3.times { read_packet(io).should be_a(MQTT::Protocol::PubRec) }
            3.times { |i| pubrel(io, (i + 1).to_u16) }
            3.times { read_packet(io).should be_a(MQTT::Protocol::PubComp) }
            disconnect(io)
          end

          3.times { |i| String.new(read_publish(sub_io).payload).should eq i.to_s }

          disconnect(sub_io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end

    it "keeps inbound QoS 2 state across a persistent reconnect [MQTT-4.4.0-1]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          # Publish-only: the session comes from CONNECT, not from a SUBSCRIBE.
          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
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
          sub_io.should be_silent

          disconnect(sub_io)
        end
      end
    end

    it "routes a re-send of a QoS 2 publish refused during the disk alarm" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          # Keeps the stats loop from starting flow again
          LavinMQ::Config.instance.free_disk_min = Int64::MAX
          server.flow(false)
          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, expect_response: false)
            io.should be_closed
          end
          LavinMQ::Config.instance.free_disk_min = 0
          server.flow(true)

          # Had the refused publish kept id 7, this would be deduped and lost
          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16, dup: true)
            pubrel(io, 7u16)
            read_packet(io).should be_a(MQTT::Protocol::PubComp)
            disconnect(io)
          end

          String.new(read_publish(sub_io).payload).should eq "1"
          sub_io.should be_silent

          disconnect(sub_io)
        end
      end
    ensure
      LavinMQ::Config.instance.free_disk_min = 0
    end

    it "drops inbound QoS 2 state on a clean session [MQTT-3.1.2-6]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "subscriber")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))

          with_client_io(server) do |io|
            connect(io, client_id: "publisher", clean_session: false)
            publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          end

          # A clean CONNECT discards the held id, so the re-send is routed
          # again. The mirror of the spec above.
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

    it "accepts a re-send at max_awaiting_pubrel but closes on a new packet id" do
      LavinMQ::Config.instance.max_awaiting_pubrel = 1u16
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          publish(io, topic: "a/b", qos: 2u8, packet_id: 1u16)
          # A held id is not a new one, so its re-send is answered with PUBREC
          publish(io, topic: "a/b", qos: 2u8, packet_id: 1u16, dup: true)
          publish(io, topic: "a/b", qos: 2u8, packet_id: 2u16, expect_response: false)
          io.should be_closed
        end
      end
    ensure
      LavinMQ::Config.instance.max_awaiting_pubrel = UInt16::MAX
    end
  end

  describe "qos2 as sender" do
    it "holds the packet id against the inflight limit until PUBCOMP" do
      LavinMQ::Config.instance.max_inflight_messages = 1u16
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            2.times do |i|
              publish(pub_io, topic: "a/b", payload: i.to_s.to_slice, qos: 2u8,
                packet_id: (i + 1).to_u16)
              pubrel(pub_io, (i + 1).to_u16)
              read_packet(pub_io).should be_a(MQTT::Protocol::PubComp)
            end
            disconnect(pub_io)
          end

          first = read_publish(io)
          String.new(first.payload).should eq "0"
          id = first.packet_id.should_not be_nil

          pubrec(io, id)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          # The window is still full: the id is owed even though the message is
          # gone, so the second message must not be delivered yet.
          io.should be_silent

          pubcomp(io, id)
          String.new(read_publish(io).payload).should eq "1"

          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end

    it "hands out the last packet id when it is the only one free" do
      # 65534 ids are held, so the window must be larger than that
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          session = server.vhosts["/"].session("mqtt.subscriber")
          # Every id but the last one handed out is held, one short of the window
          free = 100u16
          pointerof(session.@last_packet_id).value = free
          held = LavinMQ::MQTT::Session::Inflight.new(LavinMQ::MQTT::Session::Inflight::Awaiting::PubComp, nil)
          (1u16..UInt16::MAX).each { |id| session.@inflight[id] = held unless id == free }

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", payload: "a".to_slice, qos: 1u8)
            disconnect(pub_io)
          end

          read_publish(io).packet_id.should eq free
          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end

    it "parks delivery when every packet id is owed to a requeued message" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          session = server.vhosts["/"].session("mqtt.subscriber")
          store = session.@msg_store.should_not be_nil
          # Owed to messages that are not in the store, so none of them is sent
          (1u16..UInt16::MAX).each do |id|
            store.remember_original_packet_id(LavinMQ::SegmentPosition.new(99u32, id.to_u32, 1u32), id)
          end

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", payload: "a".to_slice, qos: 1u8)
            disconnect(pub_io)
          end

          # The window has room but no id is free: nothing is sent, and the
          # broker still answers
          wait_for { !session.@has_capacity.value }
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)
          disconnect(io)
        end
      end
    end

    it "encodes PUBCOMP with the reserved flags at 0 [MQTT-3.7.1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          publish(io, topic: "a/b", payload: "1".to_slice, qos: 2u8, packet_id: 7u16)
          pubrel(io, 7u16)

          # Read as bytes, not as a packet: only PUBREL, SUBSCRIBE and
          # UNSUBSCRIBE carry 0b0010. [MQTT-2.2.2-2] requires a receiver to
          # close on bad reserved bits, though mosquitto does not enforce it.
          io.read_byte.should eq 0x70u8
          io.read_byte.should eq 2u8
          io.read_int.should eq 7u16

          disconnect(io)
        end
      end
    end

    it "accepts a conformant PUBCOMP" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          pub = deliver_qos2(server, io)
          id = pub.packet_id.should_not be_nil

          pub.qos.should eq 2u8

          pubrec(io, id)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq id

          # Hand-built, not via `pubcomp`: this must assert on the bytes a real
          # client sends, not on whatever the shard encodes.
          io.write_bytes_raw(Bytes[0x70u8, 0x02u8, (id >> 8).to_u8, (id & 0xff).to_u8])
          io.should be_drained

          session = server.vhosts["/"].session("mqtt.subscriber")
          wait_for { session.@inflight.empty? }

          disconnect(io)
        end
      end
    end

    it "closes a subscriber that acknowledges a QoS 2 delivery with PUBACK, publishing its will [MQTT-4.8.0-1] [MQTT-3.1.2-8]" do
      # A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-1], so a PUBACK is a
      # protocol violation. 3.1.2.5 lists a close on a protocol error among the
      # situations in which the Will is published.
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "will-watcher")
          subscribe(watcher, topic_filters: mk_topic_filters({"w/t", 0u8}))

          with_client_io(server) do |io|
            will = MQTT::Protocol::Will.new(
              topic: "w/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io, client_id: "subscriber", will: will)
            pub = deliver_qos2(server, io)
            puback(io, pub.packet_id.should_not be_nil)
            io.should be_closed
          end

          read_publish(watcher).payload.should eq "dead".to_slice

          disconnect(watcher)
        end
      end
    end

    it "closes a subscriber that answers a QoS 1 delivery with PUBREC [MQTT-4.8.0-1]" do
      # The mirror of the PUBACK case: a QoS 1 delivery is settled by PUBACK.
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16)
            disconnect(pub_io)
          end

          pubrec(io, read_publish(io).packet_id.should_not be_nil)
          io.should be_closed
        end
      end
    end

    it "closes a subscriber that sends PUBCOMP before PUBREC [MQTT-4.8.0-1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          pub = deliver_qos2(server, io)

          # The id is booked, but still owes a PUBREC first.
          pubcomp(io, pub.packet_id.should_not be_nil)
          io.should be_closed
        end
      end
    end

    it "does not close on an acknowledgement for an id it never issued" do
      # The window does not survive a broker restart, so a resuming client
      # legitimately arrives with ids we have never seen. Unlike the wrong-type
      # case above, that is our limitation rather than the client's error.
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))

          # PUBREL is the only answer that lets the client release the id, as
          # PUBCOMP is for an unknown PUBREL.
          pubrec(io, 4242u16)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq 4242u16
          pubcomp(io, 4243u16)
          io.should be_drained

          disconnect(io)
        end
      end
    end

    it "does not reuse an unknown PUBREC's id before the client's PUBCOMP" do
      # A new session starts its ids at 1, the ones a client from before a
      # restart still holds. Our PUBREL waits for a drain, and arriving after
      # a new PUBLISH under the same id it would release that one instead.
      # Clean, so the PUBLISH itself does not wait for the drain.
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          new_id = 0u16
          with_drain_held do |gate|
            pubrec(io, 1u16)
            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "publisher")
              publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 2u8,
                packet_id: 1u16, expect_response: false)
              new_id = read_publish(io).packet_id.should_not be_nil
              new_id.should_not eq 1u16
              release_drain(gate)
              read_packet(pub_io).should be_a(MQTT::Protocol::PubRec)
            end
            read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq 1u16
            pubcomp(io, 1u16)
            pubrec(io, new_id)
            read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq new_id
            pubcomp(io, new_id)
            io.should be_drained
          end
          disconnect(io)
        end
      end
    end

    it "does not count an unknown PUBREC's id against max_inflight_messages" do
      # A client that never answers our PUBREL must not stall its deliveries.
      LavinMQ::Config.instance.max_inflight_messages = 1u16
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          pubrec(io, 1u16)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq 1u16
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 1u16)
            disconnect(pub_io)
          end
          pub = read_publish(io)
          pub.packet_id.should_not eq 1u16
          puback(io, pub.packet_id)
          pubcomp(io, 1u16)
          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end
  end

  describe "qos2 across a reconnect" do
    it "re-sends PUBREL with the original packet id on a persistent reconnect [MQTT-4.4.0-1]" do
      with_server do |server|
        owed = 0u16
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          pub = deliver_qos2(server, io)
          owed = pub.packet_id.should_not be_nil
          pubrec(io, owed)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          # Gone without a PUBCOMP, so the exchange is still open.
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          rel = read_packet(io).should be_a(MQTT::Protocol::PubRel)
          rel.packet_id.should eq owed
          # The message went at PUBREC, so a PUBLISH must not follow.
          io.should be_silent

          pubcomp(io, owed)
          disconnect(io)
        end
      end
    end

    it "does not answer a PUBREC for an id owed to a requeued message" do
      # The id is not in flight, but the requeued message will be re-sent under
      # it [MQTT-4.4.0-1]. A PUBREL now would release the id at the client,
      # which would then take that re-send for a new message.
      LavinMQ::Config.instance.max_inflight_messages = 2u16
      with_server do |server|
        rel_id = owed = 0u16
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          publish_two_qos2(server, "a/b")
          rel_id = read_publish(io).packet_id.should_not be_nil
          owed = read_publish(io).packet_id.should_not be_nil
          pubrec(io, rel_id)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          disconnect(io)
        end

        # The id awaiting PUBCOMP fills the window on reconnect, so the
        # requeued message waits under its remembered id.
        LavinMQ::Config.instance.max_inflight_messages = 1u16
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq rel_id
          io.should be_silent

          pubrec(io, owed)
          io.should be_silent

          pubcomp(io, rel_id)
          pub = read_publish(io)
          pub.packet_id.should eq owed
          pub.dup?.should be_true
          pubrec(io, owed)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq owed
          pubcomp(io, owed)
          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end

    it "keeps owing a PUBREL while the session is offline" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          pub = deliver_qos2(server, io)
          pubrec(io, pub.packet_id.should_not be_nil)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          disconnect(io)
        end

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        # Still booked against the window, but holding no message: the message
        # was deleted and acknowledged at PUBREC and only the id is owed.
        session.ack_count.should eq 1
        session.@inflight.size.should eq 1
        session.@inflight.values.first.sp.should be_nil
        session.unacked_count.should eq 0
        session.message_count.should eq 0
      end
    end

    it "does not redeliver under a packet id awaiting PUBCOMP" do
      with_server do |server|
        owed = 0u16
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          publish_two_qos2(server, "a/b")

          first = read_publish(io)
          owed = first.packet_id.should_not be_nil
          read_publish(io)
          pubrec(io, owed)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          disconnect(io)
        end

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        # Point the requeued second message at the id the PUBREL still owes, the
        # way `next_packet_id` could once its counter wraps. Reissuing it would put one
        # id on both a PUBREL and a PUBLISH.
        store = session.@msg_store.should_not be_nil
        sp = store.@original_packet_ids.keys.first
        store.@original_packet_ids[sp] = owed

        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          # [MQTT-4.4.0-1] and 4.6 order packets within each kind only, so the
          # PUBREL and the replayed publish may arrive in either order.
          packets = {read_packet(io), read_packet(io)}
          packets.count(&.is_a?(MQTT::Protocol::PubRel)).should eq 1
          rel = packets.find(&.is_a?(MQTT::Protocol::PubRel)).as(MQTT::Protocol::PubRel)
          rel.packet_id.should eq owed
          resent = packets.find(&.is_a?(MQTT::Protocol::Publish)).as(MQTT::Protocol::Publish)
          String.new(resent.payload).should eq "1"
          resent.dup?.should be_true
          resent.packet_id.should_not eq owed

          disconnect(io)
        end
      end
    end

    it "does not give a requeued message's original packet id to another message [MQTT-4.4.0-1]" do
      with_server do |server|
        owed = 0u16
        ids = Array(UInt16).new
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            3.times { |i| publish_qos2(pub_io, (i + 1).to_u16, topic: "a/b", payload: i.to_s.to_slice) }
            disconnect(pub_io)
          end
          first = read_publish(io)
          owed = first.packet_id.as(UInt16)
          2.times { ids << read_publish(io).packet_id.as(UInt16) }
          pubrec(io, owed)
          read_packet(io).should be_a(MQTT::Protocol::PubRel)
          disconnect(io)
        end

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }
        store = session.@msg_store.should_not be_nil
        # Message "1" falls back to a fresh id because its own is awaiting
        # PUBCOMP; point the counter so the fresh id would be message "2"'s.
        sp1 = store.@original_packet_ids.key_for(ids[0])
        store.@original_packet_ids[sp1] = owed
        pointerof(session.@last_packet_id).value = ids[1] &- 1

        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          packets = Array.new(3) { read_packet(io) } # the PUBREL in any position
          packets.count(&.is_a?(MQTT::Protocol::PubRel)).should eq 1
          one, two = packets.compact_map(&.as?(MQTT::Protocol::Publish))
          String.new(two.payload).should eq "2"
          two.packet_id.should eq ids[1]
          one.packet_id.should_not eq ids[1]
          disconnect(io)
        end
      end
    end

    it "sends PUBREL for the original packet id of a requeued message dropped by overflow" do
      with_server do |server|
        owed = 0u16
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          publish_two_qos2(server, "a/b")
          owed = read_publish(io).packet_id.as(UInt16)
          read_publish(io)
          disconnect(io)
        end
        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }
        # Both requeued with their original ids; drop the oldest.
        server.vhosts["/"].add_policy("ml", "^mqtt\\.resumer$", "queues", {"max-length" => JSON::Any.new(1)}, 0i8)
        wait_for { session.message_count == 1 }

        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          packets = {read_packet(io), read_packet(io)} # either order
          packets.find(&.is_a?(MQTT::Protocol::PubRel)).as(MQTT::Protocol::PubRel).packet_id.should eq owed
          String.new(packets.find(&.is_a?(MQTT::Protocol::Publish)).as(MQTT::Protocol::Publish).payload).should eq "1"
          disconnect(io)
        end
      end
    end

    it "answers a PUBREC with one PUBREL even if the original id is still remembered" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 2u8}))
          publish_two_qos2(server, "a/b")
          read_publish(io)
          read_publish(io)
          disconnect(io)
        end
        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        with_client_io(server) do |io|
          connect(io, client_id: "resumer", clean_session: false)
          first = read_publish(io)
          id = first.packet_id.as(UInt16)
          read_publish(io)
          # Recreates the window where the PUBREC is handled before the resend
          # has forgotten the remembered id.
          sp = session.@inflight[id].sp.as(LavinMQ::SegmentPosition)
          store = session.@msg_store.should_not be_nil
          store.remember_original_packet_id(sp, id)
          pubrec(io, id)
          read_packet(io).as(MQTT::Protocol::PubRel).packet_id.should eq id
          io.should be_silent
          disconnect(io)
        end
      end
    end
  end
end
