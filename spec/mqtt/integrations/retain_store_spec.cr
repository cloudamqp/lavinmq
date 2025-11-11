require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  alias IndexTree = LavinMQ::MQTT::TopicTree(String)

  context "retain_store" do
    after_each do
      # Clear out the retain_store directory
      FileUtils.rm_rf("tmp/retain_store")
    end

    describe "retain" do
      it "adds to index and writes msg file" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))
        store.retain("a", msg.body_io, msg.bodysize)

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
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))

        store.retain("a", msg.body_io, msg.bodysize)
        index.size.should eq(1)
        entry = index["a"]?.should be_a String

        store.retain("a", msg.body_io, 0)
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
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))
        store.retain("a", msg.body_io, msg.bodysize)
        10.times do
          store.each("a") do |_topic, body_io, body_bytesize|
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
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))
        store.retain("a", msg.body_io, msg.bodysize)
        msg.body_io.rewind
        store.retain("b", msg.body_io, msg.bodysize)

        called = [] of Tuple(String, Bytes)
        store.each("a") do |topic, body_io, body_bytesize|
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
        props = LavinMQ::AMQP::Properties.new
        msg1 = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))
        msg2 = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))
        store.retain("a", msg1.body_io, msg1.bodysize)
        store.retain("b", msg2.body_io, msg2.bodysize)

        called = [] of Tuple(String, Bytes)
        store.each("a") do |topic, body_io, body_bytesize|
          body = Bytes.new(body_bytesize)
          body_io.read(body)
          called << {topic, body}
        end
        store.each("b") do |topic, body_io, body_bytesize|
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
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 4, IO::Memory.new("body"))

        store.retain("a", msg.body_io, msg.bodysize)
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
      props = LavinMQ::AMQP::Properties.new
      msg = LavinMQ::Message.new(100, "test", "topic", props, 4, IO::Memory.new("body"))

      store.retain("topic", msg.body_io, msg.bodysize)
      store.close

      # Reopen
      index = IndexTree.new
      store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
      store.each("topic") do |topic, body_io, body_bytesize|
        body = Bytes.new(body_bytesize)
        body_io.read(body)
        body.should eq "body".to_slice
        topic.should eq "topic"
      end
    end

    it "subscribing to topic with retained message does not crash" do
      with_server(clean_dir: false) do |server|
        # First, publish a retained message
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          # Use a larger payload to increase chances of triggering copy_file_range
          subscribe(io, topic_filters: [subtopic("test/retain")])
          large_payload = "retained_message_" + ("x" * 8192)
          publish(io, topic: "test/retain", payload: large_payload.to_slice, qos: 0u8, retain: true)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("test/retain")])

          # Should receive the retained message without crashing
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("test/retain")
          pub.retain?.should eq(true)
          String.new(pub.payload).should start_with("retained_message_")

          disconnect(io)
        end
      end

      with_server do |server|
        # First, publish a retained message
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          # Use a larger payload to increase chances of triggering copy_file_range
          subscribe(io, topic_filters: [subtopic("test/retain")])
          large_payload = "retained_message_" + ("x" * 8192)
          publish(io, topic: "test/retain", payload: large_payload.to_slice, qos: 0u8, retain: true)
          disconnect(io)
        end
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("test/retain")])

          # Should receive the retained message without crashing
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("test/retain")
          pub.retain?.should eq(true)
          String.new(pub.payload).should start_with("retained_message_")

          disconnect(io)
        end
      end
    end
  end
end
