require "./spec_helper"

describe AMQPServer::DefaultExchange do
  it "matches exact rk" do
    log = Logger.new(File.new("/dev/null"))
    vhost = AMQPServer::VHost.new("x", "/tmp/spec", log)
    x = AMQPServer::DefaultExchange.new(vhost)
    x.queues_matching("q1").should eq(Set.new(["q1"]))
  end
end

describe AMQPServer::TopicExchange do
  log = Logger.new(File.new("/dev/null"))
  vhost = AMQPServer::VHost.new("x", "/tmp/spec", log)
  x = AMQPServer::TopicExchange.new(vhost, "t", "topic", false, false, true)

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
