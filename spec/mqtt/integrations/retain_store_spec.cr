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
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, index)
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
        store.retain("a", msg.body_io, msg.bodysize)

        index.size.should eq(1)
        index.@leafs.has_key?("a").should be_true

        entry = index["a"]?.should be_a String
        File.exists?(File.join("tmp/retain_store", entry)).should be_true
      end

      it "empty body deletes" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, index)
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
        msg2 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))

        store.retain("a", msg.body_io, msg.bodysize)
        index.size.should eq(1)
        entry = index["a"]?.should be_a String

        store.retain("a", msg.body_io, 0)
        index.size.should eq(0)
        File.exists?(File.join("tmp/retain_store", entry)).should be_false
      end
    end

    describe "each" do
      it "calls block with correct arguments" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, index)
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
        store.retain("a", msg.body_io, msg.bodysize)
        store.retain("b", msg.body_io, msg.bodysize)

        called = [] of Tuple(String, Bytes)
        store.each("a") do |topic, bytes|
          called << {topic, bytes}
        end

        called.size.should eq(1)
        called[0][0].should eq("a")
        String.new(called[0][1]).should eq("body")
      end

      it "handles multiple subscriptions" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, index)
        props = LavinMQ::AMQP::Properties.new
        msg1 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
        msg2 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
        store.retain("a", msg1.body_io, msg1.bodysize)
        store.retain("b", msg2.body_io, msg2.bodysize)

        called = [] of Tuple(String, Bytes)
        store.each("a") do |topic, bytes|
          called << {topic, bytes}
        end
        store.each("b") do |topic, bytes|
          called << {topic, bytes}
        end

        called.size.should eq(2)
        called[0][0].should eq("a")
        String.new(called[0][1]).should eq("body")
        called[1][0].should eq("b")
        String.new(called[1][1]).should eq("body")
      end
    end

    describe "restore_index" do
      it "restores the index from a file" do
        index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, index)
        props = LavinMQ::AMQP::Properties.new
        msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))

        store.retain("a", msg.body_io, msg.bodysize)
        store.close

        new_index = IndexTree.new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", LavinMQ::Clustering::NoopServer.new, new_index)

        new_index.size.should eq(1)
        new_index.@leafs.has_key?("a").should be_true
      end
    end
  end
end
