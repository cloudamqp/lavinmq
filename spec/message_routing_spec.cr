require "./spec_helper"

describe AvalancheMQ::DirectExchange do
  it "matches exact rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
    x = AvalancheMQ::DirectExchange.new(vhost, "", true, false, true)
    x.bind("q1", "q1")
    x.queues_matching("q1").should eq(Set.new(["q1"]))
  end
end

describe AvalancheMQ::TopicExchange do
  log = Logger.new(File.open("/dev/null", "w"))
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
  x = AvalancheMQ::TopicExchange.new(vhost, "t", false, false, true)

  it "matches exact rk" do
    x.bind("q1", "rk1")
    x.queues_matching("rk1").should eq(Set.new(["q1"]))
  end

  it "matches star-wildcards" do
    x.bind("q2", "*")
    x.queues_matching("rk2").should eq(Set.new(["q2"]))
  end

  it "matches star-wildcards but not too much" do
    x.bind("q2", "*")
    x.queues_matching("rk2.a").should eq(Set(String).new())
  end

  it "should not match with too many star-wildcards" do
    x.bind("q3", "a.*")
    x.queues_matching("b.c").should eq(Set(String).new)
  end

  it "should match star-wildcards in the middle" do
    x.bind("q4", "c.*.d")
    x.queues_matching("c.a.d").should eq(Set.new(["q4"]))
  end

  it "should match catch-all" do
    x.bind("q5", "d.#")
    x.queues_matching("d.a.d").should eq(Set.new(["q5"]))
  end
end

describe AvalancheMQ::HeadersExchange do
  it "can match all headers" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
    x = AvalancheMQ::HeadersExchange.new(vhost, "headers", true, false, true)
    args = Hash(String, AvalancheMQ::AMQP::Field).new
    args["x-match"] = "all"
    args["h1"] = "a"
    args["h2"] = 2
    x.bind("q1", { "x-match" => "all", "h1" => "a", "h2" => 2 })
    hdrs = Hash(String, AvalancheMQ::AMQP::Field).new
    hdrs["h1"] = "a"
    hdrs["h2"] = 2
    x.queues_matching("", hdrs).should eq(Set.new(["q1"]))
  end

  it "can match on any header" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
    x = AvalancheMQ::HeadersExchange.new(vhost, "headers", true, false, true)
    args = Hash(String, AvalancheMQ::AMQP::Field).new
    args["x-match"] = "any"
    args["h1"] = "a"
    args["h2"] = 3
    x.bind("q1", args)
    hdrs = Hash(String, AvalancheMQ::AMQP::Field).new
    hdrs["h1"] = "a"
    hdrs["h2"] = 2
    x.queues_matching("", hdrs).should eq(Set.new(["q1"]))
  end

  it "doens't return all" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
    x = AvalancheMQ::HeadersExchange.new(vhost, "headers", true, false, true)
    args = Hash(String, AvalancheMQ::AMQP::Field).new
    args["x-match"] = "any"
    args["h1"] = "a"
    args["h2"] = 3
    x.bind("q1", args)
    hdrs = Hash(String, AvalancheMQ::AMQP::Field).new
    hdrs["h1"] = "b"
    hdrs["h2"] = 2
    x.queues_matching("", hdrs).size.should eq 0
  end
end
