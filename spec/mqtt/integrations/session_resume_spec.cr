require "../spec_helper.cr"

module SessionResumeHelpers
  include MqttHelpers

  # Publishes each payload to `topic` over a throwaway connection, so the client
  # under test is only ever the subscriber.
  def publish_from(server, topic : String, payloads : Enumerable(String), qos = 1u8)
    with_client_io(server) do |io|
      connect(io, client_id: "publisher")
      payloads.each { |payload| publish(io, topic: topic, payload: payload.to_slice, qos: qos) }
      disconnect(io)
    end
  end

  def read_publishes(io, count : Int) : Array(MQTT::Protocol::Publish)
    Array(MQTT::Protocol::Publish).new(count) { read_publish(io) }
  end

  # Subscribes `client_id` at QoS 1, takes delivery of every payload without
  # acking any, then disconnects - leaving the session offline still owing them.
  # Returns the PUBLISH packets as they were delivered.
  def deliver_unacked(server, payloads : Enumerable(String),
                      topic = "a/b", client_id = "resumer") : Array(MQTT::Protocol::Publish)
    with_client_io(server) do |io|
      connect(io, client_id: client_id)
      subscribe(io, topic_filters: mk_topic_filters({topic, 1u8}))
      publish_from(server, topic, payloads)
      delivered = read_publishes(io, payloads.size)
      disconnect(io)
      delivered
    end
  end
end

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  extend SessionResumeHelpers

  describe "session resume" do
    it "resends an unacked qos1 publish with its original packet id [MQTT-4.4.0-1]" do
      with_server do |server|
        sent = deliver_unacked(server, ["1"]).first
        sent.dup?.should be_false

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publish(io)
          resent.packet_id.should eq sent.packet_id
          resent.dup?.should be_true
          resent.qos.should eq 1u8
          String.new(resent.payload).should eq "1"

          disconnect(io)
        end
      end
    end

    it "resends unacked publishes in their original order [MQTT-4.6.0-6]" do
      with_server do |server|
        sent = deliver_unacked(server, ["0", "1", "2"])

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publishes(io, 3)
          resent.each &.dup?.should be_true
          resent.map(&.packet_id).should eq sent.map(&.packet_id)
          resent.map { |p| String.new(p.payload) }.should eq ["0", "1", "2"]

          disconnect(io)
        end
      end
    end

    it "resends the owed window before delivering anything new" do
      with_server do |server|
        sent = deliver_unacked(server, ["old"]).first
        # Must be offline first: published to a session that has not detached
        # yet, "new" would go to the dying connection and join the window.
        wait_for { server.vhosts["/"].session("mqtt.resumer").client.nil? }
        publish_from(server, "a/b", ["new"])

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publish(io)
          resent.packet_id.should eq sent.packet_id
          resent.dup?.should be_true
          String.new(resent.payload).should eq "old"

          fresh = read_publish(io)
          fresh.dup?.should be_false
          String.new(fresh.payload).should eq "new"

          disconnect(io)
        end
      end
    end

    it "resends only the publishes left unacked" do
      with_server do |server|
        second = nil

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_from(server, "a/b", ["0", "1"])

          first, second = read_publishes(io, 2)
          puback(io, first.packet_id)
          pingpong(io) # the PUBACK lands before the session detaches
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publish(io)
          resent.packet_id.should eq second.try &.packet_id
          String.new(resent.payload).should eq "1"
          read_packet(io).should be_nil

          disconnect(io)
        end
      end
    end

    it "resends nothing once the resent publish is acked" do
      with_server do |server|
        deliver_unacked(server, ["1"])

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          puback(io, read_publish(io).packet_id)
          pingpong(io)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          read_packet(io).should be_nil
          disconnect(io)
        end
      end
    end

    it "keeps the original ids across a second reconnect" do
      with_server do |server|
        sent = deliver_unacked(server, ["0", "1"])

        # Take delivery of the resend but ack none of it, so the window is
        # remembered, delivered and remembered again.
        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          read_publishes(io, 2)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publishes(io, 2)
          resent.each &.dup?.should be_true
          resent.map(&.packet_id).should eq sent.map(&.packet_id)
          resent.map { |p| String.new(p.payload) }.should eq ["0", "1"]

          disconnect(io)
        end
      end
    end

    it "drops a clean session on disconnect, leaving nothing to resend [MQTT-3.1.2-6]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "cleaner", clean_session: true)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_from(server, "a/b", ["1"])

          read_publish(io) # read, never acked: a message is in flight
          server.vhosts["/"].session("mqtt.cleaner").unacked_count.should eq 1
          disconnect(io)
        end

        # The session goes with the connection, in-flight window and all.
        wait_for { !server.vhosts["/"].session_exists?("mqtt.cleaner") }

        with_client_io(server) do |io|
          connect(io, client_id: "cleaner", clean_session: true)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          read_packet(io).should be_nil
          disconnect(io)
        end
      end
    end

    it "keeps owing the window while the session is offline" do
      with_server do |server|
        deliver_unacked(server, ["1"])

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        # Requeued on detach, so still ready and still counted - only the packet
        # id is held aside for the resend.
        session.message_count.should eq 1
        session.unacked_count.should eq 0
        session.redeliver_count.should eq 0
        session.in_use?.should be_true

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          read_publish(io)
          # Counted after the yielding send, so the client can have the packet
          # before the session has counted it.
          wait_for { session.redeliver_count == 1 }
          disconnect(io)
        end
      end
    end

    # The two paths that can drop a message the session still owes, taking the
    # remembered packet id with it.
    it "forgets an owed id when the session is purged" do
      with_server do |server|
        deliver_unacked(server, ["1"])

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        session.purge.should eq 1
        session.@msg_store.@packet_ids.should be_empty

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          read_packet(io).should be_nil
          disconnect(io)
        end
      end
    end

    it "forgets an owed id when a max-length policy drops its message" do
      with_server do |server|
        deliver_unacked(server, ["1"])

        vhost = server.vhosts["/"]
        session = vhost.session("mqtt.resumer")
        wait_for { session.client.nil? }
        session.message_count.should eq 1

        defs = {"max-length" => JSON::Any.new(0_i64)} of String => JSON::Any
        vhost.add_policy("ml", "^mqtt\\.", "queues", defs, 10_i8, apply: false)
        vhost.apply_policies

        session.message_count.should eq 0
        session.@msg_store.@packet_ids.should be_empty
      end
    end

    it "does not resend under packet id 0 [MQTT-2.3.1-5]" do
      with_server do |server|
        deliver_unacked(server, ["1"])

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        # `next_id` hands out 0 once the sequence wraps, so a real session can put
        # one in here. Reissued from memory, the client rejects the illegal id and
        # gets it again on every reconnect.
        sp = session.@msg_store.@packet_ids.keys.first
        session.@msg_store.@packet_ids[sp] = 0u16

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publish(io)
          resent.packet_id.should_not eq 0u16
          String.new(resent.payload).should eq "1"

          disconnect(io)
        end
      end
    end

    it "keeps the ids of the messages a partial purge left behind" do
      with_server do |server|
        sent = deliver_unacked(server, ["0", "1", "2"])

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }
        # A purge takes the requeued messages first, in delivery order, so "0"
        # goes and the two the client still holds ids for stay.
        session.purge(1).should eq 1
        session.@msg_store.@packet_ids.size.should eq 2

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publishes(io, 2)
          resent.map(&.packet_id).should eq sent[1..].map(&.packet_id)
          resent.map { |p| String.new(p.payload) }.should eq ["1", "2"]

          disconnect(io)
        end
      end
    end

    it "forgets the ids of every owed message a purge deletes" do
      with_server do |server|
        deliver_unacked(server, ["0", "1", "2"])

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }
        publish_from(server, "a/b", ["3", "4"])
        wait_for { session.message_count == 5 }

        # The three owed messages are requeued, and a purge takes those first, so
        # this deletes every message an id is remembered for and leaves the two
        # that were published while the session was offline.
        session.purge(3).should eq 3
        session.message_count.should eq 2
        session.@msg_store.@packet_ids.should be_empty
      end
    end

    # Two remembered sps mapped to one id, which the store cannot produce on its
    # own - `@requeued` is sp-sorted and drains before any new message.
    it "passes over a remembered id that is already in flight" do
      with_server do |server|
        sent = deliver_unacked(server, ["0", "1"])
        first_id = sent.first.packet_id.not_nil!

        session = server.vhosts["/"].session("mqtt.resumer")
        wait_for { session.client.nil? }

        # Point the second message at the first one's id. Resends go out in sp
        # order, so the first delivery books that id and the second one arrives
        # to find it taken.
        second_sp = session.@msg_store.@packet_ids.keys.last
        session.@msg_store.@packet_ids[second_sp] = first_id

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publishes(io, 2)
          resent.map { |p| String.new(p.payload) }.should eq ["0", "1"]
          resent.first.packet_id.should eq first_id
          # Both still booked, under different ids. The booking now precedes the
          # yielding send, so this holds by the time the client has the packets;
          # `wait_for` is kept because it costs nothing and does not depend on
          # that ordering.
          resent.last.packet_id.should_not eq first_id
          wait_for { session.@unacked.size == 2 }

          disconnect(io)
        end
      end
    end

    it "counts the resent window against the inflight limit" do
      LavinMQ::Config.instance.max_inflight_messages = 3u16
      with_server do |server|
        sent = [] of UInt16?

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_from(server, "a/b", ["0", "1", "2", "3"])

          sent = read_publishes(io, 3).map &.packet_id
          read_packet(io).should be_nil
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "resumer")

          resent = read_publishes(io, 3)
          resent.each &.dup?.should be_true
          resent.map(&.packet_id).should eq sent
          # The window is still full, so the fourth message stays in the store.
          read_packet(io).should be_nil

          # Acking one frees a slot, and the fourth is delivered with a fresh id.
          puback(io, resent.first.packet_id)
          fourth = read_publish(io)
          String.new(fourth.payload).should eq "3"
          sent.should_not contain fourth.packet_id

          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end
  end
end
