require "./spec_helper"

module LavinMQ
  class Exchange
    # Monkey patch for backward compability and easier testing
    def matches(routing_key, headers = nil) : Set(Queue | Exchange)
      s = Set(Queue | Exchange).new
      queue_matches(routing_key, headers) { |q| s << q }
      exchange_matches(routing_key, headers) { |x| s << x }
      s
    end
  end
end

describe LavinMQ::DirectExchange do
  it "matches exact rk" do
    with_amqp_server do |s|
      vhost = s.vhosts.create("x")
      q1 = LavinMQ::Queue.new(vhost, "q1")
      x = LavinMQ::DirectExchange.new(vhost, "")
      x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
      x.matches("q1").should eq(Set{q1})
    end
  end

  it "matches no rk" do
    with_amqp_server do |s|
      vhost = s.vhosts.create("x")
      x = LavinMQ::DirectExchange.new(vhost, "")
      x.matches("q1").should be_empty
    end
  end
end

describe LavinMQ::FanoutExchange do
  it "matches any rk" do
    with_amqp_server do |s|
      vhost = s.vhosts.create("x")
      q1 = LavinMQ::Queue.new(vhost, "q1")
      x = LavinMQ::FanoutExchange.new(vhost, "")
      x.bind(q1, "")
      x.matches("any").should eq(Set{q1})
    end
  end

  it "matches no rk" do
    with_amqp_server do |s|
      vhost = s.vhosts.create("x")
      x = LavinMQ::FanoutExchange.new(vhost, "")
      x.matches("q1").should be_empty
    end
  end
end

describe LavinMQ::TopicExchange do
  with_amqp_server do |s|
    vhost = s.vhosts.create("x")
    x = LavinMQ::TopicExchange.new(vhost, "t", false, false, true)

    it "matches prefixed star-wildcard" do
      q1 = LavinMQ::Queue.new(vhost, "q1")
      x.bind(q1, "*.test")
      x.matches("rk2.test").should eq(Set{q1})
      x.unbind(q1, "*.test")
    end

    it "matches exact rk" do
      q1 = LavinMQ::Queue.new(vhost, "q1")
      x.bind(q1, "rk1")
      x.matches("rk1", nil).should eq(Set{q1})
      x.unbind(q1, "rk1")
    end

    it "matches star-wildcards" do
      q2 = LavinMQ::Queue.new(vhost, "q2")
      x.bind(q2, "*")
      x.matches("rk2").should eq(Set{q2})
      x.unbind(q2, "*")
    end

    it "matches star-wildcards but not too much" do
      q22 = LavinMQ::Queue.new(vhost, "q22")
      x.bind(q22, "*")
      x.matches("rk2.a").should be_empty
      x.unbind(q22, "*")
    end

    it "should not match with too many star-wildcards" do
      q3 = LavinMQ::Queue.new(vhost, "q3")
      x.bind(q3, "a.*")
      x.matches("b.c").should be_empty
      x.unbind(q3, "a.*")
    end

    it "should match star-wildcards in the middle" do
      q4 = LavinMQ::Queue.new(vhost, "q4")
      x.bind(q4, "c.*.d")
      x.matches("c.a.d").should eq(Set{q4})
      x.unbind(q4, "c.*.d")
    end

    it "should match catch-all" do
      q5 = LavinMQ::Queue.new(vhost, "q5")
      x.bind(q5, "d.#")
      x.matches("d.a.d").should eq(Set{q5})
      x.unbind(q5, "d.#")
    end

    it "should match multiple bindings" do
      q6 = LavinMQ::Queue.new(vhost, "q6")
      q7 = LavinMQ::Queue.new(vhost, "q7")
      ex = LavinMQ::TopicExchange.new(vhost, "t55", false, false, true)
      ex.bind(q6, "rk")
      ex.bind(q7, "rk")
      ex.matches("rk").should eq(Set{q6, q7})
      ex.unbind(q6, "rk")
      ex.unbind(q7, "rk")
    end

    it "should not get index out of bound when matching routing keys" do
      q8 = LavinMQ::Queue.new(vhost, "q63")
      ex = LavinMQ::TopicExchange.new(vhost, "t63", false, false, true)
      ex.bind(q8, "rk63.rk63")
      ex.matches("rk63").should be_empty
      ex.unbind(q8, "rk63.rk63")
    end

    it "# should consider what's comes after" do
      q9 = LavinMQ::Queue.new(vhost, "q9")
      x.bind(q9, "#.a")
      x.matches("a.a.b").should be_empty
      x.matches("a.a.a").should eq(Set{q9})
      x.unbind(q9, "#.a")
    end

    it "# can be followed by *" do
      q0 = LavinMQ::Queue.new(vhost, "q0")
      x.bind(q0, "#.*.d")
      x.matches("a.d.a").should be_empty
      x.matches("a.a.d").should eq(Set{q0})
      x.unbind(q0, "#.*.d")
    end

    it "can handle multiple #" do
      q11 = LavinMQ::Queue.new(vhost, "q11")
      x.bind(q11, "#.a.#")
      x.matches("a.b.a").should be_empty
      x.matches("b.b.a.b.b").should eq(Set{q11})
      x.unbind(q11, "#.a.#")
    end

    it "should match double star-wildcards" do
      q12 = LavinMQ::Queue.new(vhost, "q12")
      x.bind(q12, "c.*.*")
      x.matches("c.a.d").should eq(Set{q12})
      x.unbind(q12, "c.*.*")
    end

    it "should match triple star-wildcards" do
      q13 = LavinMQ::Queue.new(vhost, "q13")
      x.bind(q13, "c.*.*.*")
      x.matches("c.a.d.e").should eq(Set{q13})
      x.unbind(q13, "c.*.*.*")
    end

    it "can differentiate a.b.c from a.b" do
      q = LavinMQ::Queue.new(vhost, "")
      x.bind(q, "a.b.c")
      x.matches("a.b.c").should eq(Set{q})
      x.matches("a.b").should be_empty
      x.unbind(q, "a.b.c")
      x.bind(q, "a.b")
      x.matches("a.b").should eq(Set{q})
      x.matches("a.b.c").should be_empty
    end
  end
end

describe LavinMQ::HeadersExchange do
  with_amqp_server do |s|
    vhost = s.vhosts.create("x")

    x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
    before_each do
      vhost = s.vhosts.create("x")
      x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
    end

    hdrs_all = LavinMQ::AMQP::Table.new({
      "x-match" => "all",
      "org"     => "84codes",
      "user"    => "test",
    })
    hdrs_any = LavinMQ::AMQP::Table.new({
      "x-match" => "any",
      "org"     => "84codes",
      "user"    => "test",
    })

    describe "match all" do
      it "should match if same args" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q6 = LavinMQ::Queue.new(vhost, "q6")
        x.bind(q6, "", hdrs_all)
        x.matches("", hdrs_all).should eq(Set{q6})
      end

      it "should not match if not all args are the same" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q7 = LavinMQ::Queue.new(vhost, "q7")
        x.bind(q7, "", hdrs_all)
        msg_hdrs = hdrs_all.dup
        msg_hdrs.delete "x-match"
        msg_hdrs["org"] = "google"
        x.matches("", msg_hdrs).size.should eq 0
      end
    end

    describe "match any" do
      it "should match if any args are the same" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q8 = LavinMQ::Queue.new(vhost, "q8")
        x.bind(q8, "", hdrs_any)
        msg_hdrs = hdrs_any.dup
        msg_hdrs.delete "x-match"
        msg_hdrs["org"] = "google"
        x.matches("", msg_hdrs).should eq(Set{q8})
      end

      it "should not match if no args are the same" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q9 = LavinMQ::Queue.new(vhost, "q9")
        x.bind(q9, "", hdrs_any)
        msg_hdrs = hdrs_any.dup
        msg_hdrs.delete "x-match"
        msg_hdrs["org"] = "google"
        msg_hdrs["user"] = "hest"
        x.matches("", msg_hdrs).size.should eq 0
      end

      it "should match nestled amq-protocol tables" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q10 = LavinMQ::Queue.new(vhost, "q10")
        bind_hdrs = LavinMQ::AMQP::Table.new({
          "x-match" => "any",
          "tbl"     => LavinMQ::AMQP::Table.new({"foo": "bar"}),
        })
        x.bind(q10, "", bind_hdrs) # to_h because that's what's done in VHost
        msg_hdrs = bind_hdrs.clone
        msg_hdrs.delete("x-match")
        x.matches("", msg_hdrs).size.should eq 1
      end
    end

    it "should handle multiple bindings" do
      q10 = LavinMQ::Queue.new(vhost, "q10")
      x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
      hdrs1 = LavinMQ::AMQP::Table.new({"x-match" => "any", "org" => "84codes", "user" => "test"})
      hdrs2 = LavinMQ::AMQP::Table.new({"x-match" => "all", "org" => "google", "user" => "test"})

      x.bind(q10, "", hdrs1)
      x.bind(q10, "", hdrs2)
      hdrs1.delete "x-match"
      hdrs2.delete "x-match"
      x.matches("", hdrs1).should eq Set{q10}
      x.matches("", hdrs2).should eq Set{q10}
    end

    it "should handle all Field types" do
      q11 = LavinMQ::Queue.new(vhost, "q11")
      x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
      hsh = {"k" => "v"} of String => LavinMQ::AMQP::Field
      arrf = [1] of LavinMQ::AMQP::Field
      arru = [1_u8] of LavinMQ::AMQP::Field
      hdrs = LavinMQ::AMQP::Table.new({
        "Nil" => nil, "Bool" => true, "UInt8" => 1_u8, "UInt16" => 1_u16, "UInt32" => 1_u32,
        "Int16" => 1_u16, "Int32" => 1_i32, "Int64" => 1_i64, "Float32" => 1_f32,
        "Float64" => 1_f64, "String" => "String", "Array(Field)" => arrf,
        "Array(UInt8)" => arru, "Time" => Time.utc, "Hash(String, Field)" => hsh,
        "x-match" => "all",
      })
      x.bind(q11, "", hdrs)
      x.matches("", hdrs).should eq Set{q11}
    end

    it "should handle unbind" do
      q12 = LavinMQ::Queue.new(vhost, "q12")
      x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
      hdrs1 = LavinMQ::AMQP::Table.new({
        "x-match" => "any", "org" => "84codes", "user" => "test",
      })
      hdrs2 = LavinMQ::AMQP::Table.new({
        "x-match" => "any", "user" => "test", "org" => "84codes",
      })
      x.bind(q12, "", hdrs1)
      x.unbind(q12, "", hdrs2)
      x.matches("", hdrs1).size.should eq 0
    end

    describe "match empty" do
      it "should match if both args and headers are empty" do
        x = LavinMQ::HeadersExchange.new(vhost, "h", false, false, true)
        q13 = LavinMQ::Queue.new(vhost, "q13")
        x.bind(q13, "", nil)
        x.matches("", nil).size.should eq 1
      end
    end
  end
end
