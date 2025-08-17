require "./spec_helper"
require "./../src/lavinmq/amqp/queue"
require "./../src/lavinmq/rough_time"

describe "Dead lettering" do
  q_name = "ttl"
  q_name_delayed = "ttl_delayed"
  q_name_delayed_2 = "ttl_delayed_2"

  # Verifies bugfix for Sub-table memory corruption in amq-protocol.cr
  # https://github.com/cloudamqp/amq-protocol.cr/pull/14
  it "should be able to read messages that has been dead lettered multiple times" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_delayed_2 = ch.queue(q_name_delayed_2, args: AMQP::Client::Arguments.new(
          {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name_delayed}
        ))
        q_delayed = ch.queue(q_name_delayed, args: AMQP::Client::Arguments.new(
          {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name}
        ))
        q = ch.queue(q_name)

        x = ch.default_exchange
        x.publish_confirm("ttl", q_delayed_2.name)
        msg = wait_for { q.get }

        x_death = msg.properties.headers.not_nil!["x-death"].as(Array(AMQ::Protocol::Field))
        x_death.inspect.should be_a(String) # checks that message and headers can be read
        x_death.size.should eq 2
        x_death[0].as(AMQ::Protocol::Table)["queue"].should eq q_delayed.name
        x_death[1].as(AMQ::Protocol::Table)["queue"].should eq q_delayed_2.name
      end
    end
  end

  it "should update message timestamp on publish" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      q_args = AMQ::Protocol::Table.new({
        "x-message-ttl"             => 200,
        "x-dead-letter-exchange"    => "",
        "x-dead-letter-routing-key" => "q2",
      })
      v.declare_queue("q", true, false, q_args)
      v.declare_queue("q2", true, false, AMQ::Protocol::Table.new)

      ts = RoughTime.unix_ms
      msg = LavinMQ::Message.new(ts, "", "q", AMQ::Protocol::Properties.new, 0, IO::Memory.new)

      v.publish msg

      select
      when v.queue("q2").not_nil!.empty.when_false.receive
      when timeout(1.second)
        fail "timeout: message not dead lettered?"
      end

      v.queue("q2").not_nil!.basic_get(no_ack: true) do |env|
        msg = env.message
      end

      msg.timestamp.should be > ts
    end
  end

  it "should update count in x-death" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "q_dlx"
        q = ch.queue(q_name, args: AMQP::Client::Arguments.new(
          {"x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "#{q_name}2"}
        ))
        _q2 = ch.queue("#{q_name}2", args: AMQP::Client::Arguments.new(
          {"x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name, "x-message-ttl" => 1}
        ))

        done = Channel(AMQP::Client::DeliverMessage).new
        i = 0
        q.subscribe(no_ack: false) do |env|
          env.reject
          if i == 10
            env.ack
            done.send env
          end
          i += 1
        end
        ch.default_exchange.publish_confirm("msg", q.name)

        msg = done.receive
        headers = msg.properties.headers.should_not be_nil
        x_death = headers["x-death"].as?(Array(AMQ::Protocol::Field)).should_not be_nil
        x_death_q_dlx_rejected = x_death.find do |xd|
          xd = xd.as(AMQ::Protocol::Table)
          xd["queue"] == q_name &&
            xd["reason"] == "rejected"
        end
        x_death_q_dlx_rejected = x_death_q_dlx_rejected.as?(AMQ::Protocol::Table).should_not be_nil
        x_death_q_dlx_rejected["count"].should eq 10

        x_death_q_dlx2_expired = x_death.find do |xd|
          xd = xd.as(AMQ::Protocol::Table)
          xd["queue"] == "#{q_name}2" &&
            xd["reason"] == "expired"
        end
        x_death_q_dlx2_expired = x_death_q_dlx2_expired.as?(AMQ::Protocol::Table).should_not be_nil
        x_death_q_dlx2_expired["count"].should eq 10
      end
    end
  end

  describe "Dead letter loop detection" do
    it "should allow dead lettering on first death" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q1 = ch.queue("q1", args: AMQP::Client::Arguments.new(
            {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "q2"}
          ))
          q2 = ch.queue("q2")

          ch.default_exchange.publish_confirm("msg", q1.name)
          msg = wait_for { q2.get }

          msg.should_not be_nil
          headers = msg.properties.headers.not_nil!
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          x_death.size.should eq 1
        end
      end
    end

    it "should block dead lettering on genuine cycle (multiple automatic deaths)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q1 = ch.queue("q1", args: AMQP::Client::Arguments.new(
            {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "q2"}
          ))
          q2 = ch.queue("q2", args: AMQP::Client::Arguments.new(
            {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "q1"}
          ))

          ch.default_exchange.publish_confirm("msg", q1.name)

          # Message should cycle once but then get dropped on second cycle
          sleep 0.1.seconds # Allow messages to cycle

          q1.message_count.should eq 0
          q2.message_count.should eq 0
        end
      end
    end

    it "should allow dead lettering when cycle contains rejected reason" do
      with_amqp_server do |s|
        v = s.vhosts.create("test")

        # Create queues with dead letter setup
        q1_args = AMQ::Protocol::Table.new({
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "q2",
        })
        q2_args = AMQ::Protocol::Table.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "q1",
        })
        v.declare_queue("q1", true, false, q1_args)
        v.declare_queue("q2", true, false, q2_args)

        # Create message with x-death header containing rejected reason
        headers = AMQ::Protocol::Table.new({
          "x-death" => [
            AMQ::Protocol::Table.new({
              "queue"  => "q1",
              "reason" => "rejected",
              "count"  => 1,
            }),
            AMQ::Protocol::Table.new({
              "queue"  => "q2",
              "reason" => "expired",
              "count"  => 1,
            }),
          ] of AMQ::Protocol::Field,
        })
        props = AMQ::Protocol::Properties.new(headers: headers)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "", "q2", props, 3, IO::Memory.new("msg"))

        v.publish msg

        # Should allow dead lettering because cycle contains "rejected"
        select
        when v.queue("q1").not_nil!.empty.when_false.receive
        when timeout(0.5.seconds)
          fail "timeout: message should have been dead lettered"
        end
      end
    end

    it "should not block single death to same queue" do
      with_amqp_server do |s|
        v = s.vhosts.create("test")

        # This test demonstrates the key difference:
        # A message with single death to same queue+reason should NOT be blocked
        # Only when there are multiple deaths to same queue should it be blocked

        q1_args = AMQ::Protocol::Table.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "q2",
        })
        v.declare_queue("q1", true, false, q1_args)
        v.declare_queue("q2", true, false, AMQ::Protocol::Table.new)

        # Single death entry for current queue - should NOT block
        headers = AMQ::Protocol::Table.new({
          "x-death" => [
            AMQ::Protocol::Table.new({
              "queue"    => "q1",
              "reason"   => "expired",
              "count"    => 1,
              "time"     => Time.utc,
              "exchange" => "",
            }),
          ] of AMQ::Protocol::Field,
        })
        props = AMQ::Protocol::Properties.new(headers: headers)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "", "q1", props, 4, IO::Memory.new("msg1"))

        v.publish msg

        # Should allow through (not a cycle yet)
        select
        when v.queue("q2").not_nil!.empty.when_false.receive
          # Success
        when timeout(0.5.seconds)
          fail "Single death should NOT be blocked"
        end
      end
    end

    it "should block multiple deaths to same queue" do
      with_amqp_server do |s|
        v = s.vhosts.create("test")

        q1_args = AMQ::Protocol::Table.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "q2",
        })
        v.declare_queue("q1", true, false, q1_args)
        v.declare_queue("q2", true, false, AMQ::Protocol::Table.new)
        headers = AMQ::Protocol::Table.new({
          "x-death" => [
            AMQ::Protocol::Table.new({
              "queue"    => "q1",
              "reason"   => "expired",
              "count"    => 2, # Second time
              "time"     => Time.utc,
              "exchange" => "",
            }),
            AMQ::Protocol::Table.new({
              "queue"    => "q1",
              "reason"   => "expired",
              "count"    => 1, # First time
              "time"     => Time.utc,
              "exchange" => "",
            }),
          ] of AMQ::Protocol::Field,
        })
        props = AMQ::Protocol::Properties.new(headers: headers)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "", "q1", props, 4, IO::Memory.new("msg2"))

        initial_count = v.queue("q2").not_nil!.message_count
        v.publish msg

        # Should be blocked (genuine cycle)
        sleep 0.1.seconds
        v.queue("q2").not_nil!.message_count.should eq initial_count # No new message
      end
    end
  end
end
