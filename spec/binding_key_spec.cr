require "./spec_helper"

describe LavinMQ::AMQP::BindingKey do
  describe "#properties_key" do
    it "returns ~ for empty routing key and no arguments" do
      binding_key = LavinMQ::AMQP::BindingKey.new("")
      binding_key.properties_key.should eq "~"
    end

    it "returns '<routing key>' if no arguments are provided" do
      binding_key = LavinMQ::AMQP::BindingKey.new("routing.key")
      binding_key.properties_key.should eq "routing.key"
    end

    it "returns '~<hash>' if no routing key but arguments" do
      binding_key = LavinMQ::AMQP::BindingKey.new("", LavinMQ::AMQP::Table.new({"foo" => "bar"}))
      binding_key.properties_key.should match /^~[a-zA-Z0-9=]+$/
    end

    it "returns '<routing key>~<hash>' for routing key and arguments" do
      binding_key = LavinMQ::AMQP::BindingKey.new("routing.key", LavinMQ::AMQP::Table.new({"foo" => "bar"}))
      binding_key.properties_key.should match /^routing.key~[a-zA-Z0-9=]+$/
    end

    it "returns same hash for same arguments with different orders" do
      binding_key1 = LavinMQ::AMQP::BindingKey.new("", LavinMQ::AMQP::Table.new({"foo" => "bar", "f00" => "baz"}))
      binding_key2 = LavinMQ::AMQP::BindingKey.new("", LavinMQ::AMQP::Table.new({"f00" => "baz", "foo" => "bar"}))
      binding_key1.properties_key.should eq binding_key2.properties_key
    end
  end

  describe "#hash" do
    it "returns same value for same arguments with different orders" do
      binding_key1 = LavinMQ::AMQP::BindingKey.new("", LavinMQ::AMQP::Table.new({"foo" => "bar", "f00" => "baz"}))
      binding_key2 = LavinMQ::AMQP::BindingKey.new("", LavinMQ::AMQP::Table.new({"f00" => "baz", "foo" => "bar"}))
      binding_key1.hash.should eq binding_key2.hash
    end
  end

  # properties_key caches its result in the struct, which must not make an
  # otherwise equal key compare unequal (unbinding would then fail)
  it "is equal to an identical key whose properties_key has been computed" do
    args = LavinMQ::AMQP::Table.new({"x-match" => "all", "k" => "v"})
    computed = LavinMQ::AMQP::BindingKey.new("rk", args)
    computed.hash # computes and caches properties_key
    fresh = LavinMQ::AMQP::BindingKey.new("rk", LavinMQ::AMQP::Table.new({"x-match" => "all", "k" => "v"}))
    computed.should eq fresh
    computed.hash.should eq fresh.hash
  end
end
