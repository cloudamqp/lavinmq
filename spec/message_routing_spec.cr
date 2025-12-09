require "./spec_helper"

module MessageRoutingSpec
  def self.matches(exchange : LavinMQ::Exchange, routing_key, headers = nil) : Set(LavinMQ::Destination)
    s = Set(LavinMQ::Destination).new
    qs = Set(LavinMQ::Queue).new
    es = Set(LavinMQ::Exchange).new
    exchange.find_queues(routing_key, headers, qs, es)
    qs.each { |q| s << q }
    s
  end

  describe LavinMQ::AMQP::DirectExchange do
    it "matches exact rk" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q1, "a", LavinMQ::AMQP::Table.new)
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        x.bind(q2, "a", LavinMQ::AMQP::Table.new)
        matches(x, "q1").should eq(Set{q1})
        matches(x, "a").should eq(Set{q1, q2})
        matches(x, "foo").should be_empty
      end
    end
  end

  describe LavinMQ::AMQP::FanoutExchange do
    it "matches any rk" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        x = LavinMQ::AMQP::FanoutExchange.new(vhost, "")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        x.bind(q1, "")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x.bind(q2, "")
        matches(x, "q1").should eq(Set{q1, q2})
        matches(x, "").should eq(Set{q1, q2})
      end
    end
  end

  describe LavinMQ::AMQP::TopicExchange do
    with_amqp_server do |s|
      vhost = uninitialized LavinMQ::VHost
      x = uninitialized LavinMQ::AMQP::TopicExchange

      around_each do |example|
        vhost = s.vhosts.create("x")
        x = LavinMQ::AMQP::TopicExchange.new(vhost, "t", false, false, true)
        example.run
        s.vhosts.delete("x")
      end

      it "matches prefixed star-wildcard" do
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        x.bind(q1, "*.test")
        matches(x, "rk2.test").should eq(Set{q1})
        x.unbind(q1, "*.test")
      end

      it "matches exact rk" do
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        x.bind(q1, "rk1")
        matches(x, "rk1", nil).should eq(Set{q1})
        x.unbind(q1, "rk1")
      end

      it "matches star-wildcards" do
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x.bind(q2, "*")
        matches(x, "rk2").should eq(Set{q2})
        x.unbind(q2, "*")
      end

      it "matches star-wildcards but not too much" do
        q22 = LavinMQ::QueueFactory.make(vhost, "q22")
        x.bind(q22, "*")
        matches(x, "rk2.a").should be_empty
        x.unbind(q22, "*")
      end

      it "should not match with too many star-wildcards" do
        q3 = LavinMQ::QueueFactory.make(vhost, "q3")
        x.bind(q3, "a.*")
        matches(x, "b.c").should be_empty
        x.unbind(q3, "a.*")
      end

      it "should match star-wildcards in the middle" do
        q4 = LavinMQ::QueueFactory.make(vhost, "q4")
        x.bind(q4, "c.*.d")
        matches(x, "c.a.d").should eq(Set{q4})
        x.unbind(q4, "c.*.d")
      end

      it "should match catch-all" do
        q5 = LavinMQ::QueueFactory.make(vhost, "q5")
        x.bind(q5, "d.#")
        matches(x, "d.a.d").should eq(Set{q5})
        x.unbind(q5, "d.#")
      end

      it "should match multiple bindings" do
        q6 = LavinMQ::QueueFactory.make(vhost, "q6")
        q7 = LavinMQ::QueueFactory.make(vhost, "q7")
        ex = LavinMQ::AMQP::TopicExchange.new(vhost, "t55", false, false, true)
        ex.bind(q6, "rk")
        ex.bind(q7, "rk")
        matches(ex, "rk").should eq(Set{q6, q7})
        ex.unbind(q6, "rk")
        ex.unbind(q7, "rk")
      end

      it "should not get index out of bound when matching routing keys" do
        q8 = LavinMQ::QueueFactory.make(vhost, "q63")
        ex = LavinMQ::AMQP::TopicExchange.new(vhost, "t63", false, false, true)
        ex.bind(q8, "rk63.rk63")
        matches(ex, "rk63").should be_empty
        ex.unbind(q8, "rk63.rk63")
      end

      it "# should consider what's comes after" do
        q9 = LavinMQ::QueueFactory.make(vhost, "q9")
        x.bind(q9, "#.a")
        matches(x, "a.a.b").should be_empty
        matches(x, "a.a.a").should eq(Set{q9})
        x.unbind(q9, "#.a")
      end

      it "# should consider what's comes after" do
        q9 = LavinMQ::QueueFactory.make(vhost, "q9")
        x.bind(q9, "#.a.a.a")
        matches(x, "a.a.b.a.a").should be_empty
        matches(x, "a.a.a.a").should eq(Set{q9})
        x.unbind(q9, "#.a.a.a")
      end
      it "# should consider what's comes after" do
        q9 = LavinMQ::QueueFactory.make(vhost, "q9")
        x.bind(q9, "#.a.b.c")
        matches(x, "a.a.a.a.b.c").should eq(Set{q9})
        x.unbind(q9, "#.a.b.c")
      end

      it "# can be followed by *" do
        q0 = LavinMQ::QueueFactory.make(vhost, "q0")
        x.bind(q0, "#.*.d")
        matches(x, "a.d.a").should be_empty
        matches(x, "a.a.d").should eq(Set{q0})
        x.unbind(q0, "#.*.d")
      end

      it "can handle multiple #" do
        q11 = LavinMQ::QueueFactory.make(vhost, "q11")
        x.bind(q11, "#.a.#")
        matches(x, "b.b.a.b.b").should eq(Set{q11})
        x.unbind(q11, "#.a.#")
      end

      it "should match double star-wildcards" do
        q12 = LavinMQ::QueueFactory.make(vhost, "q12")
        x.bind(q12, "c.*.*")
        matches(x, "c.a.d").should eq(Set{q12})
        x.unbind(q12, "c.*.*")
      end

      it "should not match single star on multiple segments" do
        q = LavinMQ::QueueFactory.make(vhost, "q123")
        x.bind(q, "c.*")
        matches(x, "c.a.d").should be_empty
        x.unbind(q, "c.*")
      end

      it "should match triple star-wildcards" do
        q13 = LavinMQ::QueueFactory.make(vhost, "q13")
        x.bind(q13, "c.*.*.*")
        matches(x, "c.a.d.e").should eq(Set{q13})
        x.unbind(q13, "c.*.*.*")
      end

      it "can differentiate a.b.c from a.b" do
        q = LavinMQ::QueueFactory.make(vhost, "")
        x.bind(q, "a.b.c")
        matches(x, "a.b.c").should eq(Set{q})
        matches(x, "a.b").should be_empty
        x.unbind(q, "a.b.c")
        x.bind(q, "a.b")
        matches(x, "a.b").should eq(Set{q})
        matches(x, "a.b.c").should be_empty
        x.unbind(q, "a.b")
      end
    end
  end

  describe LavinMQ::AMQP::HeadersExchange do
    with_amqp_server do |s|
      vhost = uninitialized LavinMQ::VHost
      x = uninitialized LavinMQ::AMQP::HeadersExchange

      around_each do |example|
        vhost = s.vhosts.create("x")
        x = LavinMQ::AMQP::HeadersExchange.new(vhost, "h", false, false, true)
        example.run
        s.vhosts.delete("x")
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

      describe "bind" do
        it "shouldn't care about argument order" do
          q = LavinMQ::QueueFactory.make(vhost, "q")
          args1 = {"foo": "bar", "f00": "baz"}
          args2 = {"f00": "baz", "foo": "bar"}
          x.bind(q, "", LavinMQ::AMQP::Table.new args1)
          x.bind(q, "", LavinMQ::AMQP::Table.new args2).should be_false
        end

        it "shouldn't care about argument order with many bindings" do
          q = LavinMQ::QueueFactory.make(vhost, "q")
          args1 = {"foo": "bar", "f00": "baz"}
          args2 = {"f00": "baz", "foo": "bar"}
          x.bind(q, "", LavinMQ::AMQP::Table.new args1)
          10.times do |i|
            x.bind(q, "", LavinMQ::AMQP::Table.new args1.merge({"x": i.to_s}))
          end
          x.bind(q, "", LavinMQ::AMQP::Table.new args2).should be_false
        end
      end

      describe "unbind" do
        it "shouldn't care about argument order" do
          q = LavinMQ::QueueFactory.make(vhost, "q")
          args1 = {"foo": "bar", "f00": "baz"}
          args2 = {"f00": "baz", "foo": "bar"}
          x.bind(q, "", LavinMQ::AMQP::Table.new args1)
          x.unbind(q, "", LavinMQ::AMQP::Table.new args2).should be_true
        end

        it "shouldn't care about argument order with many bindings" do
          q = LavinMQ::QueueFactory.make(vhost, "q")
          args1 = {"foo": "bar", "f00": "baz"}
          args2 = {"f00": "baz", "foo": "bar"}
          x.bind(q, "", LavinMQ::AMQP::Table.new args1)
          10.times do |i|
            x.bind(q, "", LavinMQ::AMQP::Table.new args1.merge({"x": i.to_s}))
          end
          x.unbind(q, "", LavinMQ::AMQP::Table.new args2).should be_true
        end
      end

      describe "match all" do
        it "should match if same args" do
          q6 = LavinMQ::QueueFactory.make(vhost, "q6")
          x.bind(q6, "", hdrs_all)
          matches(x, "", hdrs_all).should eq(Set{q6})
        end

        it "should not match if not all args are the same" do
          q7 = LavinMQ::QueueFactory.make(vhost, "q7")
          x.bind(q7, "", hdrs_all)
          msg_hdrs = hdrs_all.clone
          msg_hdrs.delete "x-match"
          msg_hdrs["org"] = "google"
          matches(x, "", msg_hdrs).should be_empty
        end

        it "should not match if args are missing" do
          q = LavinMQ::QueueFactory.make(vhost, "q")
          bind_hdrs = hdrs_all.clone.merge!({
            "missing": "header",
          })
          x.bind(q, "", bind_hdrs)
          matches(x, "", hdrs_all).should be_empty
        end
      end

      describe "match any" do
        it "should match if any args are the same" do
          q8 = LavinMQ::QueueFactory.make(vhost, "q8")
          x.bind(q8, "", hdrs_any)
          msg_hdrs = hdrs_any.clone
          msg_hdrs.delete "x-match"
          msg_hdrs["org"] = "google"
          matches(x, "", msg_hdrs).should eq(Set{q8})
        end

        it "should not match if no args are the same" do
          q9 = LavinMQ::QueueFactory.make(vhost, "q9")
          x.bind(q9, "", hdrs_any)
          msg_hdrs = hdrs_any.clone
          msg_hdrs.delete "x-match"
          msg_hdrs["org"] = "google"
          msg_hdrs["user"] = "hest"
          matches(x, "", msg_hdrs).should be_empty
        end

        it "should match nestled amq-protocol tables" do
          q10 = LavinMQ::QueueFactory.make(vhost, "q10")
          bind_hdrs = LavinMQ::AMQP::Table.new({
            "x-match" => "any",
            "tbl"     => LavinMQ::AMQP::Table.new({"foo": "bar"}),
          })
          x.bind(q10, "", bind_hdrs) # to_h because that's what's done in VHost
          msg_hdrs = bind_hdrs.clone
          msg_hdrs.delete("x-match")
          matches(x, "", msg_hdrs).size.should eq 1
        end
      end

      it "should handle multiple bindings" do
        q10 = LavinMQ::QueueFactory.make(vhost, "q10")
        hdrs1 = LavinMQ::AMQP::Table.new({"x-match" => "any", "org" => "84codes", "user" => "test"})
        hdrs2 = LavinMQ::AMQP::Table.new({"x-match" => "all", "org" => "google", "user" => "test"})

        x.bind(q10, "", hdrs1)
        x.bind(q10, "", hdrs2)
        hdrs1.delete "x-match"
        hdrs2.delete "x-match"
        matches(x, "", hdrs1).should eq Set{q10}
        matches(x, "", hdrs2).should eq Set{q10}
      end

      it "should handle all Field types" do
        q11 = LavinMQ::QueueFactory.make(vhost, "q11")
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
        matches(x, "", hdrs).should eq Set{q11}
      end

      it "should handle unbind" do
        q12 = LavinMQ::QueueFactory.make(vhost, "q12")
        hdrs1 = LavinMQ::AMQP::Table.new({
          "x-match" => "any", "org" => "84codes", "user" => "test",
        })
        hdrs2 = LavinMQ::AMQP::Table.new({
          "x-match" => "any", "user" => "test", "org" => "84codes",
        })
        x.bind(q12, "", hdrs1)
        x.unbind(q12, "", hdrs2)
        matches(x, "", hdrs1).should be_empty
      end

      describe "match empty" do
        it "should match if both args and headers are empty" do
          q13 = LavinMQ::QueueFactory.make(vhost, "q13")
          x.bind(q13, "", nil)
          matches(x, "", nil).size.should eq 1
        end
      end
    end
  end

  describe LavinMQ::AMQP::Exchange do
    it "should handle CC in header" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["CC"] = ["q2"]
        matches(x, "q1", headers).should eq(Set{q1, q2})
      end
    end
    it "should raise if CC header isn't array" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["CC"] = "q2"
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          x.find_queues("q1", headers, Set(LavinMQ::Queue).new)
        end
      end
    end

    it "should handle BCC in header" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["BCC"] = ["q2"]
        matches(x, "q1", headers).should eq(Set{q1, q2})
      end
    end

    it "should raise if BCC header isn't array" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["BCC"] = "q2"
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          x.find_queues("q1", headers, Set(LavinMQ::Queue).new)
        end
      end
    end

    it "should drop BCC from header" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["BCC"] = ["q2"]
        x.find_queues("q1", headers, Set(LavinMQ::Queue).new)
        headers["BCC"]?.should be_nil
      end
    end

    it "should read both CC and BCC" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        q2 = LavinMQ::QueueFactory.make(vhost, "q2")
        q3 = LavinMQ::QueueFactory.make(vhost, "q3")
        x = LavinMQ::AMQP::DirectExchange.new(vhost, "")
        x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        x.bind(q2, "q2", LavinMQ::AMQP::Table.new)
        x.bind(q3, "q3", LavinMQ::AMQP::Table.new)
        headers = LavinMQ::AMQP::Table.new
        headers["CC"] = ["q2"]
        headers["BCC"] = ["q3"]
        matches(x, "q1", headers).should eq(Set{q1, q2, q3})
      end
    end
  end

  describe LavinMQ::MQTT::Exchange do
    it "should only allow Session to bind" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1 = LavinMQ::QueueFactory.make(vhost, "q1")
        s1 = LavinMQ::QueueFactory.make(vhost, "q1", arguments: LavinMQ::AMQP::Table.new({"x-queue-type": "mqtt"}))
        index = LavinMQ::MQTT::TopicTree(String).new
        store = LavinMQ::MQTT::RetainStore.new("tmp/retain_store", nil, index)
        x = LavinMQ::MQTT::Exchange.new(vhost, "", store)
        x.bind(s1, "s1", LavinMQ::AMQP::Table.new)
        expect_raises(LavinMQ::Exchange::AccessRefused) do
          x.bind(q1, "q1", LavinMQ::AMQP::Table.new)
        end
        store.close
      end
    end
  end
end
