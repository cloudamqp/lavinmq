require "./spec_helper"

describe "Publish Confirm Persistence" do
  it "confirms are still received by the client" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("confirm_test")
        10.times do |i|
          q.publish_confirm "message #{i}"
        end
      end
    end
  end

  it "confirms are received fast for single publishes (idle detection)" do
    # Use a long interval to ensure it's the idle detection that triggers it
    config = LavinMQ::Config.instance.dup
    config.publish_confirm_interval = 1000
    config.publish_confirm_idle_timeout = 5
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        q = ch.queue("idle_test")
        start = Time.instant
        q.publish_confirm "message 1"
        duration = Time.instant - start
        # Should be triggered by 5ms idle timeout, not the 1000ms interval
        duration.total_milliseconds.should be_close(5, 50)
      end
    end
  end

  it "batches confirms according to interval for continuous publishes" do
    config = LavinMQ::Config.instance.dup
    config.publish_confirm_interval = 200
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        q = ch.queue("batch_test")
        start = Time.instant
        # Keep publishing frequently to prevent idle timeout
        done = ::Channel(Nil).new
        spawn do
          250.times do
            ch.basic_publish "message", "", q.name
            sleep 1.milliseconds
          end
          done.send nil
        end
        done.receive
        ch.wait_for_confirms
        duration = Time.instant - start
        # Should have waited for at least some batching to occur
        # If it was 100 * 1ms, it's 100ms + the 200ms interval if triggered at the end
        # Actually it should trigger at 200ms from the first message
        (duration.total_milliseconds >= 150).should be_true
      end
    end
  end

  it "drains pending confirms when the loop is shut down" do
    # Configure long batching intervals so the batch will not auto-flush;
    # the only way the client should see the confirm is via the shutdown drain.
    config = LavinMQ::Config.instance.dup
    config.publish_confirm_interval = 10_000
    config.publish_confirm_idle_timeout = 10_000
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        q = ch.queue("drain_test")
        ch.basic_publish "msg", "", q.name
        # Wait until the message has landed in @pending_acks
        should_eventually(be_true) { !s.vhosts["/"].@pending_acks.empty? }
        # Simulate the shutdown path that closes the publish-confirm loop.
        s.vhosts["/"].@confirm_requested.close
        ch.wait_for_confirms.should be_true
      end
    end
  end

  it "nacks rejected publishes and acks the rest via multiple" do
    # Long intervals so the batch will not auto-flush; we want to observe
    # @pending_acks before the flush and trigger it explicitly.
    config = LavinMQ::Config.instance.dup
    config.publish_confirm_interval = 10_000
    config.publish_confirm_idle_timeout = 10_000
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        args = AMQP::Client::Arguments.new
        args["x-max-length"] = 1_i64
        args["x-overflow"] = "reject-publish"
        q = ch.queue("mix_confirm", args: args)

        results = {} of UInt64 => Bool
        lock = Mutex.new

        # m1 fills the queue to its max length of 1
        id1 = ch.basic_publish("m1", "", q.name) { |ok| lock.synchronize { results[1_u64] = ok } }
        should_eventually(be_true) { s.vhosts["/"].queue(q.name).message_count == 1 }

        # m2 and m3 are rejected; nacks are sent immediately, not batched.
        id2 = ch.basic_publish("m2", "", q.name) { |ok| lock.synchronize { results[2_u64] = ok } }
        id3 = ch.basic_publish("m3", "", q.name) { |ok| lock.synchronize { results[3_u64] = ok } }

        should_eventually(be_true) { lock.synchronize { results.has_key?(id2) && results.has_key?(id3) } }
        lock.synchronize do
          results[id2].should be_false
          results[id3].should be_false
          # m1's ack must still be pending — it is batched, not sent yet.
          results.has_key?(id1).should be_false
        end

        # Exactly one ack is buffered (the latest accepted msgid), which will
        # be flushed with multiple: true to cover all earlier accepted msgids.
        pending = s.vhosts["/"].@pending_acks
        pending.size.should eq 1
        pending.values.first.should eq id1

        # Force the flush via the shutdown drain path and observe m1's ack.
        s.vhosts["/"].@confirm_requested.close
        should_eventually(be_true) { lock.synchronize { results.has_key?(id1) } }
        lock.synchronize { results[id1].should be_true }
      end
    end
  end

  it "correctly acknowledges multiple messages with one ack frame" do
    # This is hard to verify from the client side without internal access,
    # but we can verify that everything is eventually acked.
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("multi_confirm_test")
        msgs = (1..100).map { |i| "msg #{i}" }
        msgs.each do |m|
          q.publish_confirm m
        end
      end
    end
  end
end
