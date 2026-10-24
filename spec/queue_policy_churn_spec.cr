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

# Hold a failing operation open so another update can arrive before it raises.
class FailingPolicyChurnQueue < LavinMQ::AMQP::Queue
  property fail_operation : Symbol? = nil
  getter operation_entered = Channel(Nil).new(1)
  getter continue_operation = Channel(Nil).new(1)

  private def fail_if_armed(operation)
    return unless @fail_operation == operation
    @fail_operation = nil
    @operation_entered.send(nil)
    @continue_operation.receive
    raise "simulated #{operation} failure"
  end

  private def drop_overflow(dlx_tasks : LavinMQ::AMQP::Argument::DeadLettering::Tasks? = nil) : Nil
    fail_if_armed(:overflow)
    super
  end

  private def drop_redelivered : Nil
    fail_if_armed(:redelivered)
    super
  end
end

# Pause after the expiration loop exits, before its worker releases the guard.
class GatedPolicyExpireQueue < LavinMQ::AMQP::Queue
  property? pause_expiry = false
  property? fail_expiry = false
  getter expiry_exited = Channel(Nil).new(1)
  getter continue_expiry = Channel(Nil).new(1)

  private def queue_expire_loop
    super
    if @pause_expiry
      @pause_expiry = false
      @expiry_exited.send(nil)
      @continue_expiry.receive
      raise "simulated queue_expire_loop failure" if @fail_expiry
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

  it "enforces delivery-limit even when the same update's overflow pass fails" do
    with_amqp_server do |s|
      queue = FailingPolicyChurnQueue.create(s.vhosts["/"], "overflow_failure",
        arguments: LavinMQ::AMQP::Table.new({"x-delivery-limit" => 10}))
      3.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
      queue.basic_get(false) { |env| queue.reject(env.segment_position, requeue: true) }.should be_true
      queue.fail_operation = :overflow
      queue.apply_policy(churn_policy({"max-length" => 100, "delivery-limit" => 0}), nil)
      queue.operation_entered.receive
      queue.continue_operation.send(nil)
      should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
      queue.message_count.should eq 2
    ensure
      queue.try &.continue_operation.try_send?(nil)
      queue.try &.delete
    end
  end

  {:overflow, :redelivered}.each do |operation|
    it "preserves an update arriving during a failing #{operation} pass" do
      with_amqp_server do |s|
        queue = FailingPolicyChurnQueue.create(s.vhosts["/"], "pending_failure")
        10.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
        queue.fail_operation = operation
        queue.apply_policy(churn_policy({"max-length" => 8}), nil)
        queue.operation_entered.receive
        queue.apply_policy(churn_policy({"max-length" => 2}), nil)
        queue.continue_operation.send(nil)
        should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
        queue.message_count.should eq 2
        queue.@policy_limits_pending.get.should be_false
      ensure
        queue.try &.continue_operation.try_send?(nil)
        queue.try &.delete
      end
    end
  end

  it "releases the limits guard after a store error and enforces policies after restart" do
    with_amqp_server do |s|
      queue = LavinMQ::AMQP::DurableQueue.create(s.vhosts["/"], "store_failure")
      3.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
      queue.@msg_store.close
      queue.apply_policy(churn_policy({"max-length" => 1}), nil)
      should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
      queue.@policy_limits_pending.get.should be_false

      queue.close
      queue.restart!.should be_true
      should_eventually(eq 1) { queue.message_count }
      should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
    ensure
      queue.try &.delete
    end
  end

  it "coalesces into a limits worker blocked across restart and uses the new policy" do
    with_amqp_server do |s|
      queue = LavinMQ::AMQP::DurableQueue.create(s.vhosts["/"], "limits_restart")
      10.times { queue.publish(LavinMQ::Message.new("", queue.name, "body")) }
      queue.@msg_store_lock.synchronize do
        queue.apply_policy(churn_policy({"max-length" => 1}), nil)
        should_eventually(be_false) { queue.@policy_limits_pending.get }
        queue.close
        queue.apply_policy(churn_policy({"max-length" => 8}), nil)
        queue.restart!.should be_true
        20.times { queue.reapply_policy }
        Fiber.yield
        workers = 0
        Fiber.list do |fiber|
          workers += 1 if fiber.name == "Queue#apply_policy_limits #{queue.vhost.name}/#{queue.name}"
        end
        workers.should eq 1
      end
      should_eventually(be_false) { queue.@policy_limits_fiber_active.get }
      queue.message_count.should eq 8
    ensure
      queue.try &.delete
    end
  end

  it "keeps one expiration worker across restart while waiting for the vhost to open" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.closed.set(true)
      queue = LavinMQ::AMQP::DurableQueue.create(vhost, "expiry_restart",
        arguments: LavinMQ::AMQP::Table.new({"x-expires" => 60_000}))
      vhost.register_queue(queue)
      Fiber.yield
      queue.close
      queue.restart!.should be_true
      queue_expire_fibers(queue).should eq 1
      vhost.closed.set(false)
      queue.apply_policy(churn_policy({"expires" => 100}), nil)
      should_eventually(be_true) { queue.closed? }
      should_eventually(eq 0) { queue_expire_fibers(queue) }
    ensure
      s.try &.vhosts["/"].closed.set(false)
      queue.try &.delete
    end
  end

  {false, true}.each do |fail_expiry|
    it "preserves expiration reapplied during teardown (failure: #{fail_expiry})" do
      with_amqp_server do |s|
        queue = GatedPolicyExpireQueue.create(s.vhosts["/"], "expiry_teardown")
        s.vhosts["/"].register_queue(queue)
        queue.apply_policy(churn_policy({"expires" => 60_000}), nil)
        Fiber.yield
        queue.pause_expiry = true
        queue.fail_expiry = fail_expiry
        queue.clear_policy
        queue.expiry_exited.receive
        queue.apply_policy(churn_policy({"expires" => 100}), nil)
        queue.@queue_expire_fiber_active.get.should be_true
        queue_expire_fibers(queue).should eq 1
        queue.continue_expiry.send(nil)
        should_eventually(be_true) { queue.closed? }
        should_eventually(eq 0) { queue_expire_fibers(queue) }
      ensure
        queue.try &.continue_expiry.try_send?(nil)
        queue.try &.delete
      end
    end
  end
end

describe "Queue policy re-apply" do
  it "keeps enforcing policy limits while the policy is re-applied" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.declare_queue("reapply_limits", durable: true, auto_delete: false)
      queue = vhost.queue("reapply_limits").as(LavinMQ::AMQP::Queue)
      queue.apply_policy(churn_policy({"max-length" => 1, "overflow" => "reject-publish"}), nil)
      queue.publish(LavinMQ::Message.new("", queue.name, "body")).should eq LavinMQ::AMQP::Queue::PublishResult::Ok
      accepted = Atomic(Int32).new(0)
      stop = Atomic(Bool).new(false)
      # Re-applying a policy must never expose the queue without its limits,
      # so publishes on other threads keep being rejected.
      deadline = Time.instant + 2.seconds
      ctx = Fiber::ExecutionContext::Parallel.new("reapply-limits", 4)
      wg = WaitGroup.new
      2.times do
        wg.add(1)
        ctx.spawn do
          until Time.instant >= deadline
            queue.reapply_policy
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
            unless queue.publish(LavinMQ::Message.new("", queue.name, "body")).overflow?
              accepted.add(1)
            end
            Fiber.yield
          end
        ensure
          wg.done
        end
      end
      wg.wait
      accepted.get.should eq 0
    ensure
      queue.try &.delete
    end
  end
end
