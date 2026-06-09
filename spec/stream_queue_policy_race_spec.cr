require "./spec_helper"

# Regression: a stream queue's `drop_overflow` closes (munmaps) segment files
# and mutates the segment bookkeeping Hashes (`@segments.reject!`). Every store
# access goes through the queue's `@msg_store_lock` EXCEPT the `drop_overflow`
# calls on the policy/argument path (`apply_policy_argument`,
# `handle_arguments`). So applying a stream-applicable policy while the store is
# being used races those operations: several fibers iterate/mutate `@segments`
# (and touch segment mmaps) with no mutual exclusion. Under real parallelism
# this corrupts the store and segfaults the broker with no log line — which is
# how the stress test crashed during stream policy churn.
#
# The bug only manifests with multiple worker threads actually running in
# parallel, so the workers are spawned on a dedicated parallel execution
# context (the spec runner's default context is effectively single-threaded).
describe "stream queue policy/publish concurrency" do
  it "applies a stream policy concurrently with publishing without corrupting the store" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.declare_queue("stream-race", true, false,
        LavinMQ::AMQP::Table.new({
          "x-queue-type"       => "stream",
          "x-max-length-bytes" => 8_388_608_i64,
        }))
      q = vhost.queue("stream-race")
      # Large bodies so the active segment rolls often -> frequent @segments
      # inserts to race the unlocked drop_overflow's reject! iteration.
      body = "x" * 65_536

      # A stream-applicable policy. Re-applying it re-runs drop_overflow
      # (apply_policy_argument + clear_policy -> handle_arguments), both
      # *outside* @msg_store_lock.
      policy = LavinMQ::Policy.new("race", vhost.name, /^stream-race$/,
        LavinMQ::Policy::Target::Queues,
        {"max-length-bytes" => JSON::Any.new(8_388_608_i64)}, 1_i8)

      stop = Atomic(Bool).new(false)
      errors = Atomic(Int32).new(0)
      deadline = Time.instant + 8.seconds

      ctx = Fiber::ExecutionContext::Parallel.new("stream-race-test", 6)
      wg = WaitGroup.new

      # Publishers: locked push, rolls segments (mutates @segments under lock).
      6.times do
        wg.add(1)
        ctx.spawn do
          n = 0_u64
          until stop.get
            begin
              q.publish(LavinMQ::Message.new("", q.name, body))
            rescue
              errors.add(1)
            end
            n &+= 1
            Fiber.yield if n % 8 == 0
          end
        ensure
          wg.done
        end
      end

      # Policy appliers: production spawns a fresh `apply_policies` fiber on
      # every add/delete, so several unlocked drop_overflow runs overlap. Each
      # drives the *unlocked* drop_overflow, racing the publishers and each other.
      4.times do
        wg.add(1)
        ctx.spawn do
          until Time.instant >= deadline
            begin
              q.apply_policy(policy, nil)
            rescue
              errors.add(1)
            end
            Fiber.yield
          end
        ensure
          wg.done
        end
      end

      # Stopper: end the publishers once the policy window closes.
      wg.add(1)
      ctx.spawn do
        until Time.instant >= deadline
          sleep 50.milliseconds
        end
        stop.set(true)
      ensure
        wg.done
      end

      wg.wait
      errors.get.should eq 0
    end
  end
end
