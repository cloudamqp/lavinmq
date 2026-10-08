require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  alias IndexTree = LavinMQ::MQTT::TopicTree(String)

  # A retained message as LavinMQ wrote it before the store kept a header: the
  # topic in the index and its payload alone in `<md5>.msg`.
  def self.write_legacy_retained(topic : String, payload : String) : String
    Dir.mkdir_p("tmp/retain_store")
    File.write(File.join("tmp/retain_store", "index"), "#{topic}\n", mode: "a")
    path = File.join("tmp/retain_store", "#{Digest::MD5.hexdigest(topic)}.msg")
    File.write(path, payload)
    path
  end

  context "retain_store" do
    after_each do
      # Clear out the retain_store directory
      FileUtils.rm_rf("tmp/retain_store")
    end

    describe "retain" do
      it "adds to index and writes msg file" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))

        index.size.should eq(1)
        index.@leafs.has_key?("a").should be_true

        entry = index["a"]?.should be_a String
        File.exists?(File.join("tmp/retain_store", entry)).should be_true
      ensure
        store.try &.close
      end

      it "empty body deletes" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)

        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        index.size.should eq(1)
        entry = index["a"]?.should be_a String

        store.retain(publish_packet(topic: "a", payload: Bytes.empty, retain: true))
        index.size.should eq(0)
        File.exists?(File.join("tmp/retain_store", entry)).should be_false
      ensure
        store.try &.close
      end
    end

    describe "each" do
      it "can be called multiple times" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        10.times do
          store.each("a") do |retained|
            body_io, body_bytesize = retained.body_io, retained.bodysize
            body = Bytes.new(body_bytesize)
            body_io.read(body)
            body.should eq "body".to_slice
          end
        end
      ensure
        store.try &.close
      end

      it "calls block with correct arguments" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        store.retain(publish_packet(topic: "b", payload: "body".to_slice, retain: true))

        called = [] of Tuple(String, Bytes)
        store.each("a") do |retained|
          topic, body_io, body_bytesize = retained.topic, retained.body_io, retained.bodysize
          body = Bytes.new(body_bytesize)
          body_io.read(body)
          called << {topic, body}
        end

        called.size.should eq(1)
        called[0][0].should eq("a")
        String.new(called[0][1]).should eq("body")
      ensure
        store.try &.close
      end

      it "handles multiple subscriptions" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        store.retain(publish_packet(topic: "b", payload: "body".to_slice, retain: true))

        called = [] of Tuple(String, Bytes)
        store.each("a") do |retained|
          topic, body_io, body_bytesize = retained.topic, retained.body_io, retained.bodysize
          body = Bytes.new(body_bytesize)
          body_io.read(body)
          called << {topic, body}
        end
        store.each("b") do |retained|
          topic, body_io, body_bytesize = retained.topic, retained.body_io, retained.bodysize
          body = Bytes.new(body_bytesize)
          body_io.read(body)
          called << {topic, body}
        end

        called.size.should eq(2)
        called[0][0].should eq("a")
        String.new(called[0][1]).should eq("body")
        called[1][0].should eq("b")
        String.new(called[1][1]).should eq("body")
      ensure
        store.try &.close
      end
    end

    describe "restore_index" do
      it "restores the index from a file" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)

        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        store.close

        new_index = IndexTree.new
        LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, new_index)

        new_index.size.should eq(1)
        new_index.@leafs.has_key?("a").should be_true
      end
    end

    it "survives a restart" do
      index = IndexTree.new
      store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)

      store.retain(publish_packet(topic: "topic", payload: "body".to_slice, retain: true))
      store.close

      # Reopen
      index = IndexTree.new
      store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
      store.each("topic") do |retained|
        topic, body_io, body_bytesize = retained.topic, retained.body_io, retained.bodysize
        body = Bytes.new(body_bytesize)
        body_io.read(body)
        body.should eq "body".to_slice
        topic.should eq "topic"
      end
    end

    describe "format" do
      it "keeps the publisher's QoS, the v5 properties and the publish time" do
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        props = MQTT::Protocol::PublishProperties.new
        props.content_type = "text/plain"
        props.user_properties = [{"k", "v"}]
        before = RoughTime.unix_ms
        store.retain(MQTT::Protocol::Publish.new("a", "body".to_slice, qos: 1u8, retain: true,
          packet_id: 1u16, properties: props))

        seen = 0
        store.each("a") do |retained|
          seen += 1
          retained.properties.delivery_mode.should eq 1u8
          retained.properties.headers.try(&.[LavinMQ::MQTT::RETAIN_HEADER]?).should be_true
          restored = LavinMQ::MQTT::PublishHeaders.restore(retained.properties.headers)
          restored.content_type.should eq "text/plain"
          restored.user_properties.should eq [{"k", "v"}]
          retained.timestamp.should be >= before
          retained.body_io.read_string(retained.bodysize).should eq "body"
        end
        seen.should eq 1
      ensure
        store.try &.close
      end

      it "reads a legacy payload-only file as QoS 1 without properties" do
        write_legacy_retained("a", "old")
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)

        seen = 0
        store.each("a") do |retained|
          seen += 1
          retained.properties.delivery_mode.should eq 1u8
          retained.body_io.read_string(retained.bodysize).should eq "old"
        end
        seen.should eq 1
      ensure
        store.try &.close
      end

      it "replaces a legacy file the next time its topic is retained" do
        legacy = write_legacy_retained("a", "old")
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        store.retain(publish_packet(topic: "a", payload: "new".to_slice, retain: true))

        File.exists?(legacy).should be_false
        bodies = [] of String
        store.each("a") { |retained| bodies << retained.body_io.read_string(retained.bodysize) }
        bodies.should eq ["new"]
      ensure
        store.try &.close
      end

      it "prefers the new file over a legacy one left by a crash, and deletes it" do
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        store.retain(publish_packet(topic: "a", payload: "new".to_slice, retain: true))
        store.close
        # As if the process died between writing the new file and deleting the
        # legacy one.
        legacy = File.join("tmp/retain_store", "#{Digest::MD5.hexdigest("a")}.msg")
        File.write(legacy, "old")

        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        File.exists?(legacy).should be_false
        bodies = [] of String
        store.each("a") { |retained| bodies << retained.body_io.read_string(retained.bodysize) }
        bodies.should eq ["new"]
      ensure
        store.try &.close
      end

      it "skips a file it cannot read instead of raising" do
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        store.retain(publish_packet(topic: "a", payload: "body".to_slice, retain: true))
        store.retain(publish_packet(topic: "b", payload: "body".to_slice, retain: true))
        store.close
        File.write(File.join("tmp/retain_store", "#{Digest::MD5.hexdigest("a")}.rmsg"), "\x07garbage")

        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, IndexTree.new)
        topics = [] of String
        store.each("#") { |retained| topics << retained.topic }
        topics.should eq ["b"]
      ensure
        store.try &.close
      end

      it "discards a retained message past its Message Expiry Interval (§3.3.1.3)", tags: "slow" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        props = MQTT::Protocol::PublishProperties.new
        props.message_expiry_interval = 1u32
        store.retain(MQTT::Protocol::Publish.new("a", "body".to_slice, retain: true, properties: props))
        entry = index["a"]?.should be_a String
        sleep 1.1.seconds

        seen = 0
        store.each("a") { seen += 1 }
        seen.should eq 0
        index.size.should eq 0
        File.exists?(File.join("tmp/retain_store", entry)).should be_false
      ensure
        store.try &.close
      end
    end

    it "subscribing to topic with retained message does not crash" do
      with_server(clean_dir: false) do |server|
        # clean_session and QoS 1 are load-bearing. A persistent session keeps
        # its binding across connections, so the subscriber would get this
        # message live as well as from the retain store; and QoS 0 is
        # unacknowledged, so nothing would order the publish before the next
        # connection. Either way a live delivery can precede the SUBACK, which
        # is legal (spec 3.8.4) and which read_packet would misread below.
        with_client_io(server) do |io|
          connect(io, client_id: "publisher", clean_session: true)
          # A larger payload increases the chances of triggering copy_file_range
          large_payload = "retained_message_" + ("x" * 8192)
          publish(io, topic: "test/retain", payload: large_payload.to_slice, qos: 1u8, retain: true)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("test/retain")])

          # Should receive the retained message without crashing
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("test/retain")
          pub.retain?.should be_true
          String.new(pub.payload).should start_with("retained_message_")

          disconnect(io)
        end
      end

      with_server do |server|
        # Same server dir as the block above, so this asserts the retained
        # message survived the restart. Same clean_session/QoS 1 reasoning.
        with_client_io(server) do |io|
          connect(io, client_id: "publisher", clean_session: true)
          # A larger payload increases the chances of triggering copy_file_range
          large_payload = "retained_message_" + ("x" * 8192)
          publish(io, topic: "test/retain", payload: large_payload.to_slice, qos: 1u8, retain: true)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("test/retain")])

          # Should receive the retained message without crashing
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("test/retain")
          pub.retain?.should be_true
          String.new(pub.payload).should start_with("retained_message_")

          disconnect(io)
        end
      end
    end
  end
end
