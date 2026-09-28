require "./spec_helper"

# Pause after an overflow pass so a stricter policy can arrive while the
# existing worker is still running.
class PolicyChurnQueue < LavinMQ::AMQP::Queue
  getter overflow_done = Channel(Nil).new(1)
  getter continue_policy = Channel(Nil).new
  property? pause_policy = false

  private def drop_overflow(dlx_tasks : LavinMQ::AMQP::Argument::DeadLettering::Tasks? = nil) : Nil
    super
    if @pause_policy
      @pause_policy = false
      @overflow_done.send(nil)
      @continue_policy.receive
    end
  end
end

private def churn_policy(definition)
  LavinMQ::Policy.new("churn", "/", /.*/, LavinMQ::Policy::Target::Queues,
    JSON.parse(definition.to_json).as_h, 0i8)
end

private def queue_expire_fibers(queue)
  count = 0
  Fiber.list do |fiber|
    count += 1 if fiber.name == "Queue#queue_expire_loop #{queue.vhost.name}/#{queue.name}"
  end
  count
end

describe "Queue policy churn" do
  it "keeps one expiration fiber across argument and policy updates with an active consumer" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("expire_churn", args: AMQP::Client::Arguments.new({"x-expires" => 60_000}))
        tag = q.subscribe { }
        queue = s.vhosts["/"].queue(q.name)
        50.times do |i|
          queue.apply_policy(churn_policy({"expires" => 30_000 + i}), nil)
          Fiber.yield
        end
        queue_expire_fibers(queue).should eq 1

        queue.apply_policy(churn_policy({"expires" => 100}), nil)
        ch.basic_cancel(tag)
        should_eventually(be_true) { queue.closed? }
        should_eventually(eq 0) { queue_expire_fibers(queue) }
      end
    end
  end

  it "stops expiration on policy removal and restarts it when reapplied" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("expire_remove")
        tag = q.subscribe { }
        queue = s.vhosts["/"].queue(q.name)
        policy = churn_policy({"expires" => 100})
        queue.apply_policy(policy, nil)
        Fiber.yield
        queue.clear_policy
        should_eventually(eq 0) { queue_expire_fibers(queue) }
        queue.closed?.should be_false

        queue.apply_policy(policy, nil)
        Fiber.yield
        queue_expire_fibers(queue).should eq 1
        ch.basic_cancel(tag)
        should_eventually(be_true) { queue.closed? }
      end
    end
  end

  it "coalesces limit updates while the message store is locked and enforces the latest limits" do
    with_amqp_server do |s|
      s.vhosts["/"].declare_queue("limits_churn", durable: true, auto_delete: false)
      queue = s.vhosts["/"].queue("limits_churn").as(LavinMQ::AMQP::Queue)
      10.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
      Fiber.yield
      existing = Set(Fiber).new
      Fiber.list { |fiber| existing << fiber }

      queue.@msg_store_lock.synchronize do
        50.times do |i|
          queue.apply_policy(churn_policy({"max-length"       => 20 + i,
                                           "max-length-bytes" => 100_000,
                                           "delivery-limit"   => 5}), nil)
          Fiber.yield
        end
        queue.apply_policy(churn_policy({"max-length"       => 2,
                                         "max-length-bytes" => 100_000,
                                         "delivery-limit"   => 5}), nil)
        Fiber.yield
        # Count unnamed workers too: the regression spawned three per update.
        workers = [] of Fiber
        Fiber.list { |fiber| workers << fiber unless existing.includes?(fiber) }
        workers.size.should eq 1
        queue.message_count.should eq 10
      end
      should_eventually(eq 2) { queue.message_count }
    end
  end

  it "rechecks limits changed during an active enforcement pass" do
    with_amqp_server do |s|
      queue = PolicyChurnQueue.create(s.vhosts["/"], "limits_during_pass")
      10.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
      queue.pause_policy = true
      queue.apply_policy(churn_policy({"max-length" => 8}), nil)
      queue.overflow_done.receive
      queue.message_count.should eq 8
      queue.apply_policy(churn_policy({"max-length" => 2}), nil)
      queue.continue_policy.send(nil)
      should_eventually(eq 2) { queue.message_count }
    ensure
      queue.try &.delete
    end
  end

  it "stops pending limit enforcement when the queue closes" do
    with_amqp_server do |s|
      s.vhosts["/"].declare_queue("limits_close", durable: true, auto_delete: false)
      queue = s.vhosts["/"].queue("limits_close").as(LavinMQ::AMQP::Queue)
      queue.@msg_store_lock.synchronize do
        queue.apply_policy(churn_policy({"max-length" => 0, "delivery-limit" => 0}), nil)
        Fiber.yield
        queue.close
      end
      should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
    end
  end
end
