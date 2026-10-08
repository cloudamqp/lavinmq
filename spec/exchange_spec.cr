require "./spec_helper"

describe LavinMQ::Exchange do
  describe "binding arguments" do
    {
      {"x-consistent-hash", "1"},
      {"direct", "routing.key"},
      {"fanout", "routing.key"},
      {"headers", "routing.key"},
      {"topic", "routing.key"},
    }.each do |exchange_type, routing_key|
      describe "exchange #{exchange_type}" do
        it "are saved" do
          with_amqp_server do |s|
            with_channel(s) do |ch|
              x = ch.exchange("test", exchange_type)
              q = ch.queue("q")
              q.bind(x.name, routing_key, args: LavinMQ::AMQP::Table.new({"x-foo": "bar"}))
              ex = s.vhosts["/"].exchange("test")
              q = s.vhosts["/"].queue("q")
              bd = ex.bindings_details.find { |b| b.destination == q }.should_not be_nil
              bd.binding_key.arguments.should eq LavinMQ::AMQP::Table.new({"x-foo": "bar"})
            end
          end
        end

        it "arguments must match when unbinding" do
          with_amqp_server do |s|
            with_channel(s) do |ch|
              x = ch.exchange("test", exchange_type)
              ch_q = ch.queue("q")
              bd_args = LavinMQ::AMQP::Table.new({"x-foo": "bar"})
              ch_q.bind(x.name, routing_key, args: bd_args)
              ex = s.vhosts["/"].exchange("test")
              q = s.vhosts["/"].queue("q")
              ex.bindings_details.find { |b| b.destination == q }.should_not be_nil
              ch_q.unbind(x.name, routing_key)
              ex.bindings_details.find { |b| b.destination == q }.should_not be_nil
              ch_q.unbind(x.name, routing_key, args: bd_args)
              ex.bindings_details.find { |b| b.destination == q }.should be_nil
            end
          end
        end
      end
    end
  end
  describe "Exchange => Exchange binding" do
    it "should allow multiple e2e bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")

          q1 = ch.queue
          q1.bind(x2.name, "#")

          x1.publish "test message", "some-rk"

          q1.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q1.get(no_ack: true).should be_nil

          x3 = ch.exchange("e3", "topic", auto_delete: true)
          x3.bind(x1.name, "#")

          q2 = ch.queue
          q2.bind(x3.name, "#")

          x1.publish "test message", "some-rk"

          q1.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q1.get(no_ack: true).should be_nil

          q2.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q2.get(no_ack: true).should be_nil
        end
      end
    end
  end

  describe "metrics" do
    x_name = "metrics"
    it "should count unroutable metrics" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x_args = AMQP::Client::Arguments.new
          x = ch.exchange(x_name, "topic", args: x_args)
          x.publish_confirm "test message 1", "none"
          s.vhosts["/"].exchange(x_name).unroutable_count.should eq 1
        end
      end
    end

    it "should count unroutable metrics" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x_args = AMQP::Client::Arguments.new
          x = ch.exchange(x_name, "topic", args: x_args)
          q = ch.queue
          q.bind(x.name, q.name)
          x.publish_confirm "test message 1", "none"
          x.publish_confirm "test message 2", q.name
          s.vhosts["/"].exchange(x_name).unroutable_count.should eq 1
        end
      end
    end
  end
  describe "auto delete exchange" do
    it "should delete the exchange when the last binding is removed" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange("ad", "topic", auto_delete: true)
          q = ch.queue
          q.bind(x.name, q.name)
          q2 = ch.queue
          q2.bind(x.name, q2.name)
          q.unbind(x.name, q.name)
          q2.unbind(x.name, q2.name)
          expect_raises(AMQP::Client::Channel::ClosedException) do
            ch.exchange("ad", "topic", passive: true)
          end
        end
      end
    end
  end

  describe "delayed message exchange declaration" do
    dmx_args = AMQP::Client::Arguments.new({"x-delayed-type" => "topic", "test" => "hello"})
    illegal_dmx_args = AMQP::Client::Arguments.new({"test" => "hello"})

    it "should declare delayed message exchange" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test", "x-delayed-message", args: dmx_args)
        end
      end
    end

    it "should raise and not declare delayed message exchange if missing x-delayed-type argument" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
            ch.exchange("test2", "x-delayed-message", args: illegal_dmx_args)
          end
          s.vhosts["/"].exchange?("test2").should be_nil
        end
      end
    end

    it "should redeclare same delayed message exchange" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test3", "x-delayed-message", args: dmx_args)
          ch.exchange("test3", "x-delayed-message", args: dmx_args)
        end
      end
    end

    it "should raise exception when redeclaring exchange with mismatched arguments" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test4", "x-delayed-message", args: dmx_args)
          expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
            ch.exchange("test4", "x-delayed-message", args: illegal_dmx_args)
          end
        end
      end
    end
  end

  describe "in_use?" do
    it "should not be in use when just created" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("e1", "topic", auto_delete: true)
          ch.exchange("e2", "topic", auto_delete: true)
          s.vhosts["/"].exchange("e1").in_use?.should be_false
          s.vhosts["/"].exchange("e2").in_use?.should be_false
        end
      end
    end
    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchange("e2").in_use?.should be_true
        end
      end
    end
    it "should be in use when other exchange has binding to it" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchange("e1").in_use?.should be_true
        end
      end
    end

    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic")
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchange("e1").in_use?.should be_true
          x2.unbind(x1.name, "#")
          s.vhosts["/"].exchange("e1").in_use?.should be_false
        end
      end
    end
  end
  describe "message deduplication" do
    it "should handle message deduplication" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-message-deduplication" => true,
          })
          ch.exchange("test", "topic", args: args)
          ch.queue.bind("test", "#")
          ex = s.vhosts["/"].exchange("test")
          q = s.vhosts["/"].queues.first
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "x-deduplication-header" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).routed?.should be_true
          ex.dedup_count.should eq 0
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "x-deduplication-header" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).routed?.should be_false
          ex.dedup_count.should eq 1

          q.message_count.should eq 1
        end
      end
    end

    it "should handle message deduplication, on custom header" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-message-deduplication" => true,
            "x-deduplication-header"  => "custom",
          })
          ch.exchange("test", "topic", args: args)
          ch.queue.bind("test", "#")
          ex = s.vhosts["/"].exchange("test")
          q = s.vhosts["/"].queues.first
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "custom" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).routed?.should be_true
          ex.dedup_count.should eq 0
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "custom" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).routed?.should be_false
          ex.dedup_count.should eq 1

          q.message_count.should eq 1
        end
      end
    end

    describe "#apply_policy" do
      describe "without federation-upstream" do
        it "stop existing link" do
          with_amqp_server do |s|
            downstream_vhost = s.vhosts.create("downstream")
            config = {"uri": JSON::Any.new("#{s.amqp_server.url}/upstream")}
            downstream_vhost.upstreams.create_upstream("upstream", config)
            definition = {"federation-upstream" => JSON::Any.new("upstream")}
            downstream_vhost.add_policy("fed", "^amq.topic", "exchanges", definition, 1i8)
            wait_for(100.milliseconds) { downstream_vhost.upstreams.@upstreams["upstream"]?.try &.links.present? }

            downstream_vhost.delete_policy("fed")
            wait_for(100.milliseconds) { downstream_vhost.upstreams.@upstreams["upstream"]?.try &.links.empty? }
          end
        end
      end
    end

    describe "with federation-upstream" do
      it "will start link" do
        with_amqp_server do |s|
          downstream_vhost = s.vhosts.create("downstream")
          config = {"uri": JSON::Any.new("#{s.amqp_server.url}/upstream")}
          downstream_vhost.upstreams.create_upstream("upstream", config)
          definition = {"federation-upstream" => JSON::Any.new("upstream")}
          downstream_vhost.add_policy("fed", "^amq.topic", "exchanges", definition, 1i8)
          wait_for(100.milliseconds) { downstream_vhost.upstreams.@upstreams["upstream"]?.try &.links.present? }
        end
      end

      describe "when exchange is internal" do
        it "won't start link" do
          with_amqp_server do |s|
            downstream_vhost = s.vhosts.create("downstream")
            config = {"uri": JSON::Any.new("#{s.amqp_server.url}/upstream")}
            downstream_vhost.upstreams.create_upstream("upstream", config)
            definition = {"federation-upstream" => JSON::Any.new("upstream")}
            downstream_vhost.add_policy("fed", "^fed", "exchanges", definition, 1i8)
            downstream_vhost.declare_exchange("fed.internal", "topic", durable: true, auto_delete: false, internal: true)
            wait_for(100.milliseconds) { downstream_vhost.exchange("fed.internal").policy.try &.name == "fed" }
            downstream_vhost.upstreams.@upstreams["upstream"].links.empty?.should be_true
          end
        end
      end
    end
  end
end

describe LavinMQ::AMQP::BindingSet do
  it "adds and deletes bindings without changing earlier versions" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.declare_queue("bs-a", false, false)
      vhost.declare_queue("bs-b", false, false)
      a = vhost.queue("bs-a")
      b = vhost.queue("bs-b")
      key = LavinMQ::AMQP::BindingKey.new("rk")
      empty = LavinMQ::AMQP::BindingSet.empty
      one = empty.add(a, key).not_nil!
      two = one.add(b, key).not_nil!
      one.add(a, key).should be_nil # already bound
      two.delete(a, LavinMQ::AMQP::BindingKey.new("other")).should be(two)
      after = two.delete(a, key)
      [empty.size, one.size, two.size, after.size].should eq [0, 1, 2, 1]
      dests = [] of LavinMQ::AMQP::Destination
      after.each_destination { |d| dests << d }
      dests.should eq [b]
    end
  end

  it "switches between an array and a persistent map by size" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      max = LavinMQ::AMQP::BindingSet::ARRAY_MAX
      queues = Array(LavinMQ::AMQP::Queue).new(max + 1) do |i|
        vhost.declare_queue("bs-q#{i}", false, false)
        vhost.queue("bs-q#{i}")
      end
      key = LavinMQ::AMQP::BindingKey.new("")
      set = LavinMQ::AMQP::BindingSet.empty
      queues.each { |q| set = set.add(q, key).not_nil! }
      set.should be_a LavinMQ::AMQP::MapBindingSet
      set.size.should eq max + 1
      set.add(queues.first, key).should be_nil
      # Shrinks back to an array at half the limit
      queues[0, max // 2 + 1].each { |q| set = set.delete(q, key) }
      set.should be_a LavinMQ::AMQP::ArrayBindingSet
      seen = Set(LavinMQ::AMQP::Destination).new
      set.each_destination { |d| seen << d }
      seen.should eq queues[max // 2 + 1..].to_set
    end
  end
end

describe "Exchange bindings under concurrency" do
  # Publishers route on several threads while binds and unbinds replace the
  # binding sets; the routing reads must never see a half-changed set
  {"direct", "topic", "fanout", "headers"}.each do |type|
    it "routes #{type} exchanges while bindings change", tags: "slow" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        x = "concurrent-#{type}"
        vhost.declare_exchange(x, type, false, false)
        args = type == "headers" ? LavinMQ::AMQP::Table.new({"x-match" => "all", "k" => "v"}) : LavinMQ::AMQP::Table.new
        headers = LavinMQ::AMQP::Table.new({"k" => "v"})
        rk = type == "topic" ? "stable.#" : "stable"
        vhost.declare_queue("stable", false, false)
        vhost.bind_queue("stable", x, rk, args)
        # Enough to cross BindingSet::ARRAY_MAX back and forth
        churn = Array(String).new(100) { |i| "churn-#{i}" }
        churn.each { |q| vhost.declare_queue(q, false, false) }
        exchange = vhost.exchanges.find! { |e| e.name == x }
        stable = vhost.queue("stable")

        stop = Atomic(Bool).new(false)
        misses = Atomic(Int32).new(0)
        ctx = Fiber::ExecutionContext::Parallel.new("bindings-#{type}", 4)
        wg = WaitGroup.new(8)
        8.times do
          ctx.spawn do
            queues = Set(LavinMQ::AMQP::Queue).new
            exchanges = Set(LavinMQ::AMQP::Exchange).new
            i = 0
            until stop.get(:relaxed)
              queues.clear
              exchanges.clear
              exchange.find_queues("stable.x", headers, queues, exchanges) if type == "topic"
              exchange.find_queues("stable", headers, queues, exchanges) unless type == "topic"
              misses.add(1) unless queues.includes?(stable)
              Fiber.yield if (i += 1) % 64 == 0
            end
          ensure
            wg.done
          end
        end
        20.times do
          churn.each { |q| vhost.bind_queue(q, x, rk, args) }
          churn.each { |q| vhost.unbind_queue(q, x, rk, args) }
        end
        stop.set(true)
        wg.wait
        misses.get.should eq 0
        exchange.binding_count.should eq 1
      end
    end
  end
end
