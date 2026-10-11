require "./spec_helper"

describe "Alternate Exchange" do
  it "only routes to alternate-exchange when no queues are bound" do
    with_amqp_server do |s|
      args = AMQP::Client::Arguments.new
      args["alternate-exchange"] = "unroutables"

      with_channel(s) do |ch|
        topic_ex = ch.exchange("topic-with-ae", "topic", args: args)
        unroutables_ex = ch.exchange("unroutables", "topic")
        unroutables_q = ch.queue("unroutables")
        unroutables_q.bind(unroutables_ex.name, "*")

        # When we publish to topic_ex without bindings, the message should go thought the alternate exchange
        topic_ex.publish("m1", "rk")
        msg = unroutables_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m1")

        # When we publish to topic_ex with queue bindings and exchange bindings with alternate-exchange,
        # and that exchange does not have any queues bound to it,
        # the message should go through the alternate exchange and the queue
        # (same behavior as RMQ)
        secondary_ex = ch.exchange("secondary", "headers", args: args)
        ch.exchange_bind(topic_ex.name, secondary_ex.name, "#")
        target_q = ch.queue("q")
        target_q.bind(topic_ex.name, "*")
        topic_ex.publish("m3", "rk3")
        msg = target_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m3")
        msg = unroutables_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m3")
      end
    end
  end
end

describe "Alternate Exchange policy" do
  it "keeps routing to the policy's alternate exchange while the policy is re-applied" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.declare_exchange("ae-reapply", "topic", durable: false, auto_delete: false)
      vhost.declare_exchange("ae-reapply-unroutables", "fanout", durable: false, auto_delete: false)
      vhost.declare_queue("ae-reapply-unroutables", durable: false, auto_delete: false,
        arguments: LavinMQ::AMQP::Table.new({"x-max-length" => 1}))
      vhost.bind_queue("ae-reapply-unroutables", "ae-reapply-unroutables", "")
      ex = vhost.exchange("ae-reapply").as(LavinMQ::AMQP::Exchange)
      policy = LavinMQ::Policy.new("ae", "/", /^ae-reapply$/, LavinMQ::Policy::Target::Exchanges,
        {"alternate-exchange" => JSON::Any.new("ae-reapply-unroutables")}, 0i8)
      ex.apply_policy(policy, nil)
      unrouted = Atomic(Int32).new(0)
      stop = Atomic(Bool).new(false)
      deadline = Time.instant + 2.seconds
      # Re-applying a policy must never expose the exchange without its
      # alternate exchange, so publishes on other threads are never dropped.
      ctx = Fiber::ExecutionContext::Parallel.new("ae-reapply", 4)
      wg = WaitGroup.new
      2.times do
        wg.add(1)
        ctx.spawn do
          until Time.instant >= deadline
            ex.reapply_policy
            Fiber.yield
          end
        ensure
          stop.set(true)
          wg.done
        end
      end
      2.times do
        wg.add(1)
        ctx.spawn do
          # Yield, and stop at the deadline, so that the appliers aren't
          # starved on a runner with fewer cores than threads
          until stop.get || Time.instant >= deadline
            msg = LavinMQ::Message.new(ex.name, "rk", "body")
            unrouted.add(1) unless ex.route_msg(msg).routed?
            Fiber.yield
          end
        ensure
          wg.done
        end
      end
      wg.wait
      unrouted.get.should eq 0
    end
  end
end
