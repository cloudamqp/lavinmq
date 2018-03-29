require "./spec_helper"

describe AvalancheMQ::DirectExchange do
  it "matches exact rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
    x = AvalancheMQ::DirectExchange.new(vhost, "", true, false, true)
    x.bind("q1", "q1")
    x.queues_matching("q1").should eq(Set{"q1"})
  end
end

describe AvalancheMQ::TopicExchange do
  log = Logger.new(File.open("/dev/null", "w"))
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
  x = AvalancheMQ::TopicExchange.new(vhost, "t", false, false, true)

  it "matches exact rk" do
    x.bind("q1", "rk1")
    x.queues_matching("rk1").should eq(Set{"q1"})
  end

  it "matches star-wildcards" do
    x.bind("q2", "*")
    x.queues_matching("rk2").should eq(Set{"q2"})
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
    x.queues_matching("c.a.d").should eq(Set{"q4"})
  end

  it "should match catch-all" do
    x.bind("q5", "d.#")
    x.queues_matching("d.a.d").should eq(Set{"q5"})
  end
end

describe AvalancheMQ::HeadersExchange do
  log = Logger.new(STDOUT)
  # log.level = Logger::DEBUG
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)
  x = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
  hdrs_all = {
    "x-match" => "all",
    "org" => "84codes",
    "user" => "test"
  } of String => AvalancheMQ::AMQP::Field
  hdrs_any = {
    "x-match" => "any",
    "org" => "84codes",
    "user" => "test"
  } of String => AvalancheMQ::AMQP::Field


  describe "match all" do
    it "should match if same args" do
      x.bind("q6", nil, hdrs_all)
      x.queues_matching(nil, hdrs_all).should eq(Set{"q6"})
    end

    it "should not match if not all args are the same" do
      x.bind("q7", nil, hdrs_all)
      msg_hdrs = hdrs_all.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      x.queues_matching(nil, msg_hdrs).size.should eq 0
    end
  end

  describe "match any" do
    it "should match if any args are the same" do
      x.bind("q8", nil, hdrs_any)
      msg_hdrs = hdrs_any.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      x.queues_matching(nil, msg_hdrs).should eq(Set{"q8"})
    end

    it "should not match if no args are the same" do
      x.bind("q9", nil, hdrs_any)
      msg_hdrs = hdrs_any.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      msg_hdrs["user"] = "hest"
      x.queues_matching(nil, msg_hdrs).size.should eq 0
    end
  end

  it "should handle multiple bindings" do
    hx = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
    hdrs1 = { "x-match" => "any", "org" => "84codes",
              "user" => "test"} of String => AvalancheMQ::AMQP::Field
    hdrs2 = { "x-match" => "all", "org" => "google",
              "user" => "test"} of String => AvalancheMQ::AMQP::Field

    hx.bind("q10", nil, hdrs1)
    hx.bind("q10", nil, hdrs2)
    hdrs1.delete "x-match"
    hdrs2.delete "x-match"
    hx.queues_matching(nil, hdrs1).should eq Set{"q10"}
    hx.queues_matching(nil, hdrs2).should eq Set{"q10"}
  end

  it "should handle all Field types" do
    hsh = { "k" => "v"} of String => AvalancheMQ::AMQP::Field
    arrf = [1] of AvalancheMQ::AMQP::Field
    arru = [1_u8] of AvalancheMQ::AMQP::Field
    hdrs = { "Nil" => nil, "Bool" => true, "UInt8" => 1_u8, "UInt16" => 1_u16, "UInt32" => 1_u32,
             "UInt64" => 1_u64, "Int32" => 1_i32, "Int64" => 1_i64, "Float32" => 1_f32,
             "Float64" => 1_f64, "String" => "String", "Array(Field)" => arrf,
             "Array(UInt8)" => arru, "Time" => Time.now, "Hash(String, Field)" => hsh,
             "x-match" => "all"
           } of String => AvalancheMQ::AMQP::Field
    x.bind("q11", nil, hdrs)
    x.queues_matching(nil, hdrs).should eq Set{"q11"}
  end

  it "should handle unbind" do
    hx = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
    hdrs1 = { "x-match" => "any", "org" => "84codes",
              "user" => "test"} of String => AvalancheMQ::AMQP::Field
    hx.bind("q12", nil, hdrs1)
    hx.unbind("q12", nil)
    hx.queues_matching(nil, hdrs1).size.should eq 0
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
