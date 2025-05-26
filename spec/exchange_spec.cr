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
              ex = s.vhosts["/"].exchanges["test"]
              q = s.vhosts["/"].queues["q"]
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
              ex = s.vhosts["/"].exchanges["test"]
              q = s.vhosts["/"].queues["q"]
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
          s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
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
          s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
        end
      end
    end
  end
  describe "auto delete exchange" do
    it "should delete the exhchange when the last binding is removed" do
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
          s.vhosts["/"].exchanges["test2"]?.should be_nil
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
          s.vhosts["/"].exchanges["e1"].in_use?.should be_false
          s.vhosts["/"].exchanges["e2"].in_use?.should be_false
        end
      end
    end
    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e2"].in_use?.should be_true
        end
      end
    end
    it "should be in use when other exchange has binding to it" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_true
        end
      end
    end

    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic")
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_true
          x2.unbind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_false
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
          ex = s.vhosts["/"].exchanges["test"]
          q = s.vhosts["/"].queues.first_value
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "x-deduplication-header" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).should eq 1
          ex.dedup_count.should eq 0
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "x-deduplication-header" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).should eq 0
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
          ex = s.vhosts["/"].exchanges["test"]
          q = s.vhosts["/"].queues.first_value
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "custom" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).should eq 1
          ex.dedup_count.should eq 0
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "custom" => "msg1",
          }))
          msg = LavinMQ::Message.new("ex", "rk", "body", props)
          ex.publish(msg, false).should eq 0
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
            config = {"uri": JSON::Any.new("#{s.amqp_url}/upstream")}
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
          config = {"uri": JSON::Any.new("#{s.amqp_url}/upstream")}
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
            config = {"uri": JSON::Any.new("#{s.amqp_url}/upstream")}
            downstream_vhost.upstreams.create_upstream("upstream", config)
            definition = {"federation-upstream" => JSON::Any.new("upstream")}
            downstream_vhost.add_policy("fed", "^fed", "exchanges", definition, 1i8)
            downstream_vhost.declare_exchange("fed.internal", "topic", durable: true, auto_delete: false, internal: true)
            wait_for(100.milliseconds) { downstream_vhost.exchanges["fed.internal"].policy.try &.name == "fed" }
            downstream_vhost.upstreams.@upstreams["upstream"].links.empty?.should be_true
          end
        end
      end
    end
  end
end
