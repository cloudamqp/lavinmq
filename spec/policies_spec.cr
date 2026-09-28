require "./spec_helper"

class PoliciesSpec
  def self.with_vhost(&)
    with_amqp_server do |s|
      vhost = s.vhosts.create("add_policy")
      yield vhost
    end
  end
end

# Once armed, drop_overflow blocks on a gate and then raises on its next call,
# so a spec can queue another request while the coalescing fiber is mid-run.
class RaiseOnceDropOverflowQueue < LavinMQ::AMQP::Queue
  @gate = ::Channel(Nil).new
  property? armed = false

  def release_gate
    @gate.send nil
  end

  private def drop_overflow(dlx_tasks : LavinMQ::AMQP::Argument::DeadLettering::Tasks? = nil) : Nil
    if @armed
      @armed = false
      @gate.receive
      raise LavinMQ::MessageStore::ClosedError.new
    end
    super
  end
end

# Once armed, queue_expire_loop blocks on a gate after the loop has exited but
# before the spawn block's ensure clears the running flag, holding open the
# window where a concurrent apply could find the guard still set.
class GatedExpireQueue < LavinMQ::AMQP::Queue
  @gate = ::Channel(Nil).new
  property? armed = false

  def release_gate
    @gate.send nil
  end

  private def queue_expire_loop(gen : UInt32)
    super
    if @armed
      @armed = false
      @gate.receive
    end
  end
end

describe LavinMQ::VHost do
  definitions = {
    "max-length"         => JSON::Any.new(10_i64),
    "alternate-exchange" => JSON::Any.new("dead-letters"),
    "unsupported"        => JSON::Any.new("unsupported"),
    "delivery-limit"     => JSON::Any.new(10_i64),
  }

  it "should be able to add policy" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.policies.size.should eq 1
      vhost.delete_policy("test")
    end
  end

  it "should remove policy from resource when deleted" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test1").as(LavinMQ::AMQP::Queue))
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      sleep 10.milliseconds
      vhost.queue("test1").policy.try(&.name).should eq "test"
      vhost.delete_policy("test")
      sleep 10.milliseconds
      vhost.queue("test1").policy.should be_nil
    end
  end

  it "should be able to list policies" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.delete_policy("test")
      vhost.policies.size.should eq 0
    end
  end

  it "should overwrite policy with same name" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.add_policy("test", "^.*$", "exchanges", definitions, 10_i8)
      vhost.policies.size.should eq 1
      vhost.policies["test"].apply_to.should eq LavinMQ::Policy::Target::Exchanges
      vhost.delete_policy("test")
    end
  end

  it "should apply policy" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
      vhost.add_policy("ml", "^.*$", "queues", defs, 11_i8)
      sleep 10.milliseconds
      vhost.queue("test").policy.not_nil!.name.should eq "ml"
    end
  end

  it "should apply classic_queues policy only to non-stream queues" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "classic").as(LavinMQ::AMQP::Queue))
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "stream",
        arguments: LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})).as(LavinMQ::AMQP::Queue))
      vhost.add_policy("cq", "^.*$", "classic_queues", defs, 11_i8)
      sleep 10.milliseconds
      vhost.queue("classic").policy.try(&.name).should eq "cq"
      vhost.queue("stream").policy.should be_nil
    end
  end

  it "should apply streams policy only to stream queues" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-age" => JSON::Any.new("1D")} of String => JSON::Any
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "classic").as(LavinMQ::AMQP::Queue))
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "stream",
        arguments: LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})).as(LavinMQ::AMQP::Queue))
      vhost.add_policy("sp", "^.*$", "streams", defs, 11_i8)
      sleep 10.milliseconds
      vhost.queue("stream").policy.try(&.name).should eq "sp"
      vhost.queue("classic").policy.should be_nil
    end
  end

  it "should apply quorum_queues policy to non-stream queues" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"delivery-limit" => JSON::Any.new(3_i64)} of String => JSON::Any
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "classic").as(LavinMQ::AMQP::Queue))
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "stream",
        arguments: LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})).as(LavinMQ::AMQP::Queue))
      vhost.add_policy("qq", "^.*$", "quorum_queues", defs, 11_i8)
      sleep 10.milliseconds
      vhost.policies["qq"].apply_to.should eq LavinMQ::Policy::Target::QuorumQueues
      vhost.queue("classic").policy.try(&.name).should eq "qq"
      vhost.queue("stream").policy.should be_nil
    end
  end

  it "should respect priority" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
      vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test2").as(LavinMQ::AMQP::Queue))
      vhost.add_policy("ml2", "^.*$", "queues", defs, 1_i8)
      vhost.add_policy("ml1", "^.*$", "queues", defs, 0_i8)
      sleep 10.milliseconds
      vhost.queue("test2").policy.not_nil!.name.should eq "ml2"
    end
  end

  it "should remove effect of deleted policy" do
    with_amqp_server do |s|
      defs = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
      s.vhosts["/"].add_policy("mld", "^.*$", "all", defs, 12_i8)
      with_channel(s) do |ch|
        q = ch.queue("mld")
        11.times do
          q.publish_confirm "body"
        end
        ch.queue_declare("mld", passive: true)[:message_count].should eq 10
        s.vhosts["/"].delete_policy("mld")
        q.publish_confirm "body"
        ch.queue_declare("mld", passive: true)[:message_count].should eq 11
      end
    end
  end

  it "should apply message TTL policy on existing queue" do
    with_amqp_server do |s|
      defs = {"message-ttl" => JSON::Any.new(0_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("policy-ttl")
        10.times do
          q.publish_confirm "body"
        end
        ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 10
        s.vhosts["/"].add_policy("ttl", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 0
        s.vhosts["/"].delete_policy("ttl")
      end
    end
  end

  it "should apply queue TTL policy on existing queue" do
    with_amqp_server do |s|
      defs = {"expires" => JSON::Any.new(0_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("qttl")
        q.publish_confirm ""
        s.vhosts["/"].add_policy("qttl", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        expect_raises(AMQP::Client::Channel::ClosedException) do
          ch.queue_declare("qttl", passive: true)
        end
      end
    end
  end

  it "should update queue expiration" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("qttl", args: AMQP::Client::Arguments.new({"x-expires" => 1000}))
        queue = s.vhosts["/"].queue("qttl").as(LavinMQ::AMQP::Queue)
        Fiber.yield
        expire_before = queue.@expires
        expire_before.should eq 1000
        s.vhosts["/"].add_policy("qttl", "^.*$", "all", {"expires" => JSON::Any.new(100)}, 2_i8)
        Fiber.yield
        expire_after = queue.@expires
        expire_after.should eq 100
        select
        when queue.@paused.when_true.receive?
          fail "queue state not closed?" unless queue.closed?
        when timeout 500.milliseconds
          fail "queue not closed, probably not expired on time?"
        end
      end
    end
  end

  it "should apply max-length-bytes on existing queue" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 3
        sleep 20.milliseconds
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  it "should remove head if queue to large" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  it "should not enqueue messages that make the queue to large" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64),
              "overflow"         => JSON::Any.new("reject-publish")} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short1")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  it "repeated policy applies do not spawn duplicate Queue loops" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("repeat")
        q = s.vhosts["/"].queue("repeat").as(LavinMQ::AMQP::Queue)
        defs = {
          "expires"          => JSON::Any.new(60_000_i64),
          "max-length"       => JSON::Any.new(100_i64),
          "max-length-bytes" => JSON::Any.new(1_000_000_i64),
          "delivery-limit"   => JSON::Any.new(10_i64),
        } of String => JSON::Any
        loop_count = ->(needle : String) do
          c = 0
          Fiber.list do |f|
            n = f.@name
            c += 1 if n && n.includes?(needle) && n.includes?("/repeat")
          end
          c
        end
        # Hold the store lock so drop_overflow/drop_redelivered fibers stay
        # alive (blocked on the lock) and can be counted
        q.@msg_store_lock.synchronize do
          20.times do |i|
            s.vhosts["/"].add_policy("p#{i}", "^repeat$", "queues", defs, i.to_i8)
          end
          sleep 50.milliseconds
          loop_count.call("queue_expire_loop").should eq 1
          loop_count.call("drop_overflow").should eq 1
          loop_count.call("drop_redelivered").should eq 1
        end
        20.times { |i| s.vhosts["/"].delete_policy("p#{i}") }
      end
    end
  end

  it "clears drop_overflow guard when drop_overflow raises" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("raise")
        2.times { q.publish "m" }
        queue = s.vhosts["/"].queue("raise").as(LavinMQ::AMQP::Queue)
        wait_for { queue.message_count == 2 }
        # A closed store makes drop_overflow raise ClosedError on shift?
        queue.@msg_store.close
        defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
        s.vhosts["/"].add_policy("ml", "^raise$", "queues", defs, 0_i8)
        wait_for { queue.@drop_overflow_pending.get == false }
        wait_for { queue.@drop_overflow_loop_running.get == false }
      end
    end
  end

  it "services a drop_overflow request queued while a failing run was in flight" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      q = RaiseOnceDropOverflowQueue.create(vhost, "raise_once")
      3.times { q.publish(LavinMQ::Message.new("", q.name, "m", LavinMQ::AMQP::Properties.new)) }
      q.message_count.should eq 3
      q.armed = true
      p1 = LavinMQ::Policy.new("a", "/", /.*/, LavinMQ::Policy::Target::Queues,
        {"max-length" => JSON::Any.new(2_i64)}, 0_i8)
      q.apply_policy(p1, nil)
      wait_for { q.@drop_overflow_pending.get == false } # fiber is inside drop_overflow
      p2 = LavinMQ::Policy.new("b", "/", /.*/, LavinMQ::Policy::Target::Queues,
        {"max-length" => JSON::Any.new(1_i64)}, 0_i8)
      q.apply_policy(p2, nil)
      q.@drop_overflow_pending.get.should be_true
      q.release_gate
      wait_for { q.message_count == 1 }
      wait_for { q.@drop_overflow_loop_running.get == false }
    ensure
      q.try &.delete
    end
  end

  it "restart! clears policy loop guards" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("restart")
        q = s.vhosts["/"].queue("restart").as(LavinMQ::AMQP::Queue)
        q.close
        q.@drop_overflow_loop_running.set(true)
        q.@drop_redelivered_loop_running.set(true)
        q.@queue_expire_loop_running.set(true)
        q.restart!.should be_true
        q.@drop_overflow_loop_running.get.should be_false
        q.@drop_redelivered_loop_running.get.should be_false
        q.@queue_expire_loop_running.get.should be_false
      end
    end
  end

  it "stale drop_overflow fiber surviving restart! doesn't release the new fiber's guard" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        vhost = s.vhosts["/"]
        x = ch.queue("stale", durable: true)
        2.times { x.publish "m" }
        q = vhost.queue("stale").as(LavinMQ::AMQP::Queue)
        wait_for { q.message_count == 2 }
        loop_count = -> do
          c = 0
          Fiber.list do |f|
            n = f.@name
            c += 1 if n && n.includes?("drop_overflow") && n.includes?("/stale")
          end
          c
        end
        defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
        q.@msg_store_lock.synchronize do
          # Stale fiber: blocked on the store lock across close/restart!
          vhost.add_policy("ml", "^stale$", "queues", defs, 0_i8)
          wait_for { loop_count.call == 1 }
          # Park the post-restart fiber on the vhost closed gate so it stays live
          vhost.closed.set(true)
          q.close
          q.restart!.should be_true
          wait_for { loop_count.call == 2 }
        end
        # Let the stale fiber finish while the new one is still parked
        wait_for { loop_count.call == 1 }
        # A further apply must coalesce into the live fiber, not spawn another
        defs2 = {"max-length" => JSON::Any.new(2_i64)} of String => JSON::Any
        vhost.add_policy("ml2", "^stale$", "queues", defs2, 1_i8)
        sleep 50.milliseconds
        loop_count.call.should eq 1
      ensure
        s.vhosts["/"].closed.set(false)
      end
    end
  end

  it "stale queue_expire_loop surviving restart! exits instead of running a second loop" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      loop_count = -> do
        c = 0
        Fiber.list do |f|
          n = f.@name
          c += 1 if n && n.includes?("queue_expire_loop") && n.includes?("/stale_expire")
        end
        c
      end
      # Park the expire fiber on the vhost closed gate so it survives close/restart!
      vhost.closed.set(true)
      args = LavinMQ::AMQP::Table.new({"x-expires" => 60_000})
      q = LavinMQ::AMQP::DurableQueue.create(vhost, "stale_expire", arguments: args)
      wait_for { loop_count.call == 1 }
      q.close
      q.restart!.should be_true
      wait_for { loop_count.call == 2 }
      vhost.closed.set(false)
      wait_for { loop_count.call == 1 }
      sleep 50.milliseconds
      loop_count.call.should eq 1
      q.@queue_expire_loop_running.get.should be_true
    ensure
      s.try &.vhosts["/"].closed.set(false)
      q.try &.delete
    end
  end

  it "re-applying expires while the expire loop is exiting doesn't lose the loop" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      q = GatedExpireQueue.create(vhost, "gated_expire")
      loop_count = -> do
        c = 0
        Fiber.list do |f|
          n = f.@name
          c += 1 if n && n.includes?("queue_expire_loop") && n.includes?("/gated_expire")
        end
        c
      end
      expires = LavinMQ::Policy.new("e", "/", /.*/, LavinMQ::Policy::Target::Queues,
        {"expires" => JSON::Any.new(60_000_i64)}, 0_i8)
      other = LavinMQ::Policy.new("o", "/", /.*/, LavinMQ::Policy::Target::Queues,
        {"max-length" => JSON::Any.new(10_i64)}, 0_i8)
      q.apply_policy(expires, nil)
      wait_for { loop_count.call == 1 }
      sleep 10.milliseconds # let the loop park in its select
      q.armed = true
      # Clears @expires and wakes the loop, which exits and parks on the gate
      q.apply_policy(other, nil)
      wait_for { !q.armed? }
      # Re-apply expires while the exiting loop still holds the guard
      q.apply_policy(expires, nil)
      q.release_gate
      sleep 50.milliseconds
      loop_count.call.should eq 1
      q.@queue_expire_loop_running.get.should be_true
    ensure
      q.try &.delete
    end
  end

  it "should drop messages if above delivery-limit" do
    with_amqp_server do |s|
      defs = {"delivery-limit" => JSON::Any.new(0_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new
        args["x-delivery-limit"] = 5
        q = ch.queue("delivery-limit", exclusive: true, args: args)
        q.publish_confirm "m1"
        q.publish_confirm "m2"
        q.get(no_ack: false).not_nil!.reject(requeue: true)
        ch.queue_declare("delivery-limit", passive: true)[:message_count].should eq 2
        s.vhosts["/"].add_policy("delivery-limit", "^.*$", "all", defs, 12_i8)
        sleep 10.milliseconds
        ch.queue_declare("delivery-limit", passive: true)[:message_count].should eq 1
        s.vhosts["/"].delete_policy("delivery-limit")
      end
    end
  end

  describe "with max-length-bytes policy applied" do
    it "should replace with max-length" do
      with_amqp_server do |s|
        defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
        with_channel(s) do |ch|
          q = ch.queue("max-length-bytes", exclusive: true)
          s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "queues", defs, 12_i8)
          q.publish_confirm "short1"
          q.publish_confirm "short2"
          q.publish_confirm "long"
          ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2

          defs = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
          s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "queues", defs, 12_i8)
          10.times do
            q.publish_confirm "msg"
          end
          ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 10
          s.vhosts["/"].delete_policy("max-length-bytes")
        end
      end
    end
  end

  describe "operator policies" do
    it "merges with normal polices" do
      with_amqp_server do |s|
        ml_2 = {"max-length" => JSON::Any.new(2_i64)} of String => JSON::Any
        ml_1 = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
        s.vhosts["/"].add_policy("ml", ".*", "all", ml_2, 0_i8)
        with_channel(s) do |ch|
          q = ch.queue
          3.times do
            q.publish_confirm "body"
          end
          queue = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Queue)
          queue.message_count.should eq 2
          s.vhosts["/"].add_operator_policy("ml1", ".*", "all", ml_1, 0_i8)
          wait_for { queue.message_count == 1 }

          # deleting operator policy should make normal policy active again
          s.vhosts["/"].delete_operator_policy("ml1")
          3.times do
            q.publish_confirm "body"
          end
          queue.message_count.should eq 2
        end
      end
    end

    it "effective_policy_definition should not include unsupported policies" do
      PoliciesSpec.with_vhost do |vhost|
        supported_policies = {"max-length", "max-length-bytes",
                              "message-ttl", "expires", "overflow",
                              "dead-letter-exchange", "dead-letter-routing-key",
                              "federation-upstream", "federation-upstream-set",
                              "delivery-limit", "max-age", "alternate-exchange",
                              "delayed-message"}
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
        sleep 10.milliseconds
        vhost.queue("test").details_tuple[:effective_policy_definition].as(Hash(String, JSON::Any)).each_key do |k|
          supported_policies.includes?(k).should be_true
        end
        vhost.delete_policy("test")
      end
    end
  end

  describe "together with arguments" do
    it "arguments should have priority for non numeric arguments" do
      PoliciesSpec.with_vhost do |vhost|
        no_ae_ex = LavinMQ::AMQP::DirectExchange.new(vhost, "no-ae")
        vhost.register_exchange(no_ae_ex)
        ae_ex = LavinMQ::AMQP::DirectExchange.new(vhost, "x-with-ae",
          arguments: AMQ::Protocol::Table.new({"x-alternate-exchange": "ae2"}))
        vhost.register_exchange(ae_ex)
        vhost.add_policy("test", ".*", "all", definitions, 100_i8)
        sleep 10.milliseconds
        no_ae_ex.@alternate_exchange.should eq "dead-letters"
        ae_ex.@alternate_exchange.should eq "ae2"
        vhost.delete_policy("test")
        sleep 10.milliseconds
        no_ae_ex.@alternate_exchange.should be_nil
        ae_ex.@alternate_exchange.should eq "ae2"
      end
    end

    it "should use the lowest value" do
      PoliciesSpec.with_vhost do |vhost|
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test1", arguments: LavinMQ::AMQP::Table.new({"x-max-length" => 1_i64})).as(LavinMQ::AMQP::Queue))
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test2", arguments: LavinMQ::AMQP::Table.new({"x-max-length" => 11_i64})).as(LavinMQ::AMQP::Queue))
        vhost.add_policy("test", ".*", "all", definitions, 100_i8)
        sleep 10.milliseconds
        vhost.queue("test1").as(LavinMQ::AMQP::Queue).@max_length.should eq 1
        vhost.queue("test2").as(LavinMQ::AMQP::Queue).@max_length.should eq 10
        vhost.delete_policy("test")
        sleep 10.milliseconds
        vhost.queue("test1").as(LavinMQ::AMQP::Queue).@max_length.should eq 1
        vhost.queue("test2").as(LavinMQ::AMQP::Queue).@max_length.should eq 11
      end
    end

    it "should use the lowest value for delivery-limit" do
      PoliciesSpec.with_vhost do |vhost|
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test1", arguments: LavinMQ::AMQP::Table.new({"x-delivery-limit" => 1_i64})).as(LavinMQ::AMQP::Queue))
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test2", arguments: LavinMQ::AMQP::Table.new({"x-delivery-limit" => 11_i64})).as(LavinMQ::AMQP::Queue))
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test3").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("test", ".*", "all", definitions, 100_i8)
        sleep 10.milliseconds
        vhost.queue("test1").as(LavinMQ::AMQP::Queue).@delivery_limit.should eq 1
        vhost.queue("test2").as(LavinMQ::AMQP::Queue).@delivery_limit.should eq 10
        vhost.queue("test3").as(LavinMQ::AMQP::Queue).@delivery_limit.should eq 10
        vhost.delete_policy("test")
        sleep 10.milliseconds
        vhost.queue("test1").as(LavinMQ::AMQP::Queue).@delivery_limit.should eq 1
        vhost.queue("test2").as(LavinMQ::AMQP::Queue).@delivery_limit.should eq 11
        vhost.queue("test3").as(LavinMQ::AMQP::Queue).@delivery_limit.should be_nil
      end
    end
  end

  describe "handling invalid policy values" do
    it "should ignore invalid type for max-length and apply valid values" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "max-length"  => JSON::Any.new("not-a-number"),
          "message-ttl" => JSON::Any.new(5000_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-type", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@max_length.should be_nil
        queue.@message_ttl.should eq 5000
        vhost.delete_policy("invalid-type")
      end
    end

    it "should ignore invalid type for max-length-bytes" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "max-length-bytes" => JSON::Any.new(true),
          "max-length"       => JSON::Any.new(10_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-bytes", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@max_length_bytes.should be_nil
        queue.@max_length.should eq 10
        vhost.delete_policy("invalid-bytes")
      end
    end

    it "should ignore invalid type for message-ttl" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "message-ttl" => JSON::Any.new("invalid"),
          "max-length"  => JSON::Any.new(20_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-ttl", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@message_ttl.should be_nil
        queue.@max_length.should eq 20
        vhost.delete_policy("invalid-ttl")
      end
    end

    it "should ignore invalid type for expires" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "expires"    => JSON::Any.new([JSON::Any.new(1)]),
          "max-length" => JSON::Any.new(15_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-expires", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@expires.should be_nil
        queue.@max_length.should eq 15
        vhost.delete_policy("invalid-expires")
      end
    end

    it "should ignore invalid type for overflow" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "overflow"   => JSON::Any.new(123_i64),
          "max-length" => JSON::Any.new(25_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-overflow", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@reject_on_overflow.should be_false
        queue.@max_length.should eq 25
        vhost.delete_policy("invalid-overflow")
      end
    end

    it "should ignore invalid type for dead-letter-exchange" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "dead-letter-exchange" => JSON::Any.new(999_i64),
          "max-length"           => JSON::Any.new(30_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-dlx", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@dead_letter.@dlx.should be_nil
        queue.@max_length.should eq 30
        vhost.delete_policy("invalid-dlx")
      end
    end

    it "should ignore invalid type for dead-letter-routing-key" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "dead-letter-routing-key" => JSON::Any.new([JSON::Any.new("dlrk")]),
          "max-length"              => JSON::Any.new("abc"),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-dlrk", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@dead_letter.@dlx.should be_nil
        queue.@max_length.should be_nil
        vhost.delete_policy("invalid-dlrk")
      end
    end

    it "should ignore invalid type for delivery-limit" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "delivery-limit" => JSON::Any.new("five"),
          "max-length"     => JSON::Any.new(40_i64),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("invalid-limit", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@delivery_limit.should be_nil
        queue.@max_length.should eq 40
        vhost.delete_policy("invalid-limit")
      end
    end

    it "should apply all valid values when mixed with invalid types" do
      PoliciesSpec.with_vhost do |vhost|
        defs = {
          "max-length"              => JSON::Any.new(50_i64),
          "max-length-bytes"        => JSON::Any.new("invalid"),
          "message-ttl"             => JSON::Any.new(3000_i64),
          "expires"                 => JSON::Any.new(false),
          "overflow"                => JSON::Any.new("reject-publish"),
          "dead-letter-exchange"    => JSON::Any.new(123_i64),
          "dead-letter-routing-key" => JSON::Any.new("dlrk"),
          "delivery-limit"          => JSON::Any.new("bad"),
        }
        vhost.register_queue(LavinMQ::QueueFactory.make(vhost, "test").as(LavinMQ::AMQP::Queue))
        vhost.add_policy("mixed", "^test$", "queues", defs, 0_i8)
        sleep 10.milliseconds
        queue = vhost.queue("test").as(LavinMQ::AMQP::Queue)
        queue.@max_length.should eq 50
        queue.@max_length_bytes.should be_nil
        queue.@message_ttl.should eq 3000
        queue.@expires.should be_nil
        queue.@reject_on_overflow.should be_true
        queue.@dead_letter.@dlx.should be_nil
        queue.@dead_letter.@dlrk.should eq "dlrk"
        queue.@delivery_limit.should be_nil
        vhost.delete_policy("mixed")
      end
    end
  end
end
