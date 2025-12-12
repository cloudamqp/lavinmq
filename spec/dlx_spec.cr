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
      when v.queues["q2"].empty.when_false.receive
      when timeout(1.second)
        fail "timeout: message not dead lettered?"
      end

      v.queues["q2"].basic_get(no_ack: true) do |env|
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
        ch.queue("#{q_name}2", args: AMQP::Client::Arguments.new(
          {"x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name, "x-max-length" => 0}
        ))

        done = Channel(AMQP::Client::DeliverMessage).new
        i = 0
        q.subscribe(no_ack: false) do |env|
          if i == 5
            env.ack
            done.send env
          else
            env.reject(requeue: false) # reject to trigger dead lettering
          end
          i += 1
        end

        ch.default_exchange.publish_confirm("msg", q.name)

        select
        when msg = done.receive
          msg = msg.should be_a AMQP::Client::DeliverMessage
        when timeout(1.second)
          fail "Nope, timeout"
        end
        headers = msg.properties.headers.should be_a AMQ::Protocol::Table
        x_deaths = headers["x-death"].as?(Array(AMQ::Protocol::Field)).should_not be_nil
        x_death = x_deaths.find do |xd|
          xd = xd.as(AMQ::Protocol::Table)
          xd["queue"] == q_name
        end
        x_death = x_death.should be_a(AMQ::Protocol::Table)
        x_death["count"].should eq 5
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
        when v.queues["q1"].empty.when_false.receive
        when timeout(0.5.seconds)
          fail "timeout: message should have been dead lettered"
        end
      end
    end

    it "should prevent dead letter cycle but publish to other queues" do
      # Test that a message dead-letterd from q1 won't end up in q1 again (loop)
      # but it should still be dead-lettered to q2
      with_amqp_server do |s|
        v = s.vhosts.create("test")

        q1_args = AMQ::Protocol::Table.new({
          "x-max-length"              => 0,
          "x-dead-letter-exchange"    => "amq.topic",
          "x-dead-letter-routing-key" => "q2",
        })

        v.declare_queue("q1", true, false, q1_args)
        v.declare_queue("q2", true, false, AMQ::Protocol::Table.new)
        v.bind_queue("q1", "amq.topic", "#")
        v.bind_queue("q2", "amq.topic", "#")

        msg = LavinMQ::Message.new("", "q1", "foo")

        v.publish msg

        # Should allow through (not a cycle yet)
        select
        when v.queues["q2"].empty.when_false.receive
          v.queues["q1"].empty.value.should be_true
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

        initial_count = v.queues["q2"].message_count
        v.publish msg

        # Should be blocked (genuine cycle)
        sleep 0.2.seconds
        v.queues["q2"].message_count.should eq initial_count # No new message
      end
    end

    it "should prevent dead lettering loop" do
      with_amqp_server do |_|
      end
    end
  end
end
