require "./spec_helper"

describe AvalancheMQ::DirectExchange do
  it "matches exact rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
    q1 = AvalancheMQ::Queue.new(vhost, "q1")
    x = AvalancheMQ::DirectExchange.new(vhost, "")
    x.bind(q1, "q1", Hash(String, AvalancheMQ::AMQP::Field).new)
    x.matches("q1").should eq(Set{q1})
  end

  it "matches no rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
    x = AvalancheMQ::DirectExchange.new(vhost, "")
    x.matches("q1").should be_empty
  end
end

describe AvalancheMQ::FanoutExchange do
  it "matches any rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
    q1 = AvalancheMQ::Queue.new(vhost, "q1")
    x = AvalancheMQ::FanoutExchange.new(vhost, "")
    x.bind(q1, "")
    x.matches("any").should eq(Set{q1})
  end

  it "matches no rk" do
    log = Logger.new(File.open("/dev/null", "w"))
    vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
    x = AvalancheMQ::FanoutExchange.new(vhost, "")
    x.matches("q1").should be_empty
  end
end

describe AvalancheMQ::TopicExchange do
  log = Logger.new(File.open("/dev/null", "w"))
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
  x = AvalancheMQ::TopicExchange.new(vhost, "t", false, false, true)

  it "matches exact rk" do
    q1 = AvalancheMQ::Queue.new(vhost, "q1")
    x.bind(q1, "rk1")
    x.matches("rk1", nil).should eq(Set{q1})
    x.unbind(q1, "rk1")
  end

  it "matches star-wildcards" do
    q2 = AvalancheMQ::Queue.new(vhost, "q2")
    x.bind(q2, "*")
    x.matches("rk2").should eq(Set{q2})
    x.unbind(q2, "*")
  end

  it "matches star-wildcards but not too much" do
    q22 = AvalancheMQ::Queue.new(vhost, "q22")
    x.bind(q22, "*")
    x.matches("rk2.a").should be_empty
    x.unbind(q22, "*")
  end

  it "should not match with too many star-wildcards" do
    q3 = AvalancheMQ::Queue.new(vhost, "q3")
    x.bind(q3, "a.*")
    x.matches("b.c").should be_empty
    x.unbind(q3, "a.*")
  end

  it "should match star-wildcards in the middle" do
    q4 = AvalancheMQ::Queue.new(vhost, "q4")
    x.bind(q4, "c.*.d")
    x.matches("c.a.d").should eq(Set{q4})
    x.unbind(q4, "c.*.d")
  end

  it "should match catch-all" do
    q5 = AvalancheMQ::Queue.new(vhost, "q5")
    x.bind(q5, "d.#")
    x.matches("d.a.d").should eq(Set{q5})
    x.unbind(q5, "d.#")
  end

  it "should match multiple bindings" do
    q6 = AvalancheMQ::Queue.new(vhost, "q6")
    q7 = AvalancheMQ::Queue.new(vhost, "q7")
    ex = AvalancheMQ::TopicExchange.new(vhost, "t55", false, false, true)
    ex.bind(q6, "rk")
    ex.bind(q7, "rk")
    ex.matches("rk").should eq(Set{q6, q7})
    ex.unbind(q6, "rk")
    ex.unbind(q7, "rk")
  end

  it "should not get index out of bound when matching routing keys" do
    q8 = AvalancheMQ::Queue.new(vhost, "q63")
    ex = AvalancheMQ::TopicExchange.new(vhost, "t63", false, false, true)
    ex.bind(q8, "rk63.rk63")
    ex.matches("rk63").should be_empty
    ex.unbind(q8, "rk63.rk63")
  end

  it "# should consider what's comes after" do
    q9 = AvalancheMQ::Queue.new(vhost, "q9")
    x.bind(q9, "#.a")
    x.matches("a.a.b").should be_empty
    x.matches("a.a.a").should eq(Set{q9})
    x.unbind(q9, "#.a")
  end

  it "# can be followed by *" do
    q0 = AvalancheMQ::Queue.new(vhost, "q0")
    x.bind(q0, "#.*.d")
    x.matches("a.d.a").should be_empty
    x.matches("a.a.d").should eq(Set{q0})
    x.unbind(q0, "#.*.d")
  end

  it "can handle multiple #" do
    q11 = AvalancheMQ::Queue.new(vhost, "q11")
    x.bind(q11, "#.a.#")
    x.matches("a.b.a").should be_empty
    x.matches("b.b.a.b.b").should eq(Set{q11})
    x.unbind(q11, "#.a.#")
  end
end

describe AvalancheMQ::HeadersExchange do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
  x = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
  hdrs_all = {
    "x-match" => "all",
    "org"     => "84codes",
    "user"    => "test",
  } of String => AvalancheMQ::AMQP::Field
  hdrs_any = {
    "x-match" => "any",
    "org"     => "84codes",
    "user"    => "test",
  } of String => AvalancheMQ::AMQP::Field

  describe "match all" do
    it "should match if same args" do
      q6 = AvalancheMQ::Queue.new(vhost, "q6")
      x.bind(q6, "", hdrs_all)
      x.matches("", hdrs_all).should eq(Set{q6})
    end

    it "should not match if not all args are the same" do
      q7 = AvalancheMQ::Queue.new(vhost, "q7")
      x.bind(q7, "", hdrs_all)
      msg_hdrs = hdrs_all.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      x.matches("", msg_hdrs).size.should eq 0
    end
  end

  describe "match any" do
    it "should match if any args are the same" do
      q8 = AvalancheMQ::Queue.new(vhost, "q8")
      x.bind(q8, "", hdrs_any)
      msg_hdrs = hdrs_any.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      x.matches("", msg_hdrs).should eq(Set{q8})
    end

    it "should not match if no args are the same" do
      q9 = AvalancheMQ::Queue.new(vhost, "q9")
      x.bind(q9, "", hdrs_any)
      msg_hdrs = hdrs_any.dup
      msg_hdrs.delete "x-match"
      msg_hdrs["org"] = "google"
      msg_hdrs["user"] = "hest"
      x.matches("", msg_hdrs).size.should eq 0
    end
  end

  it "should handle multiple bindings" do
    q10 = AvalancheMQ::Queue.new(vhost, "q10")
    hx = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
    hdrs1 = {"x-match" => "any", "org" => "84codes",
             "user" => "test"} of String => AvalancheMQ::AMQP::Field
    hdrs2 = {"x-match" => "all", "org" => "google",
             "user" => "test"} of String => AvalancheMQ::AMQP::Field

    hx.bind(q10, "", hdrs1)
    hx.bind(q10, "", hdrs2)
    hdrs1.delete "x-match"
    hdrs2.delete "x-match"
    hx.matches("", hdrs1).should eq Set{q10}
    hx.matches("", hdrs2).should eq Set{q10}
  end

  it "should handle all Field types" do
    q11 = AvalancheMQ::Queue.new(vhost, "q11")
    hsh = {"k" => "v"} of String => AvalancheMQ::AMQP::Field
    arrf = [1] of AvalancheMQ::AMQP::Field
    arru = [1_u8] of AvalancheMQ::AMQP::Field
    hdrs = {"Nil" => nil, "Bool" => true, "UInt8" => 1_u8, "UInt16" => 1_u16, "UInt32" => 1_u32,
            "Int16" => 1_u16, "Int32" => 1_i32, "Int64" => 1_i64, "Float32" => 1_f32,
            "Float64" => 1_f64, "String" => "String", "Array(Field)" => arrf,
            "Array(UInt8)" => arru, "Time" => Time.now, "Hash(String, Field)" => hsh,
            "x-match" => "all",
    } of String => AvalancheMQ::AMQP::Field
    x.bind(q11, "", hdrs)
    x.matches("", hdrs).should eq Set{q11}
  end

  it "should handle unbind" do
    q12 = AvalancheMQ::Queue.new(vhost, "q12")
    hx = AvalancheMQ::HeadersExchange.new(vhost, "h", false, false, true)
    hdrs1 = {"x-match" => "any", "org" => "84codes",
             "user" => "test"} of String => AvalancheMQ::AMQP::Field
    hx.bind(q12, "", hdrs1)
    hx.unbind(q12, "", hdrs1)
    hx.matches("", hdrs1).size.should eq 0
  end
end
