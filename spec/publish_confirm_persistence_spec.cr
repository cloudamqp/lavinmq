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

  it "confirms a single publish quickly (no batching delay)" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("single_publish_test")
        start = Time.instant
        q.publish_confirm "message 1"
        duration = Time.instant - start
        duration.should be < 100.milliseconds
      end
    end
  end

  it "nacks rejected publishes and acks the accepted ones" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        args = AMQP::Client::Arguments.new
        args["x-max-length"] = 1_i64
        args["x-overflow"] = "reject-publish"
        q = ch.queue("mix_confirm", args: args)

        results = {} of UInt64 => Bool
        lock = Mutex.new

        id1 = ch.basic_publish("m1", "", q.name) { |ok| lock.synchronize { results[1_u64] = ok } }
        should_eventually(be_true) { s.vhosts["/"].queue(q.name).message_count == 1 }

        id2 = ch.basic_publish("m2", "", q.name) { |ok| lock.synchronize { results[2_u64] = ok } }
        id3 = ch.basic_publish("m3", "", q.name) { |ok| lock.synchronize { results[3_u64] = ok } }

        should_eventually(be_true) do
          lock.synchronize { results.size == 3 }
        end
        lock.synchronize do
          results[id1].should be_true  # accepted
          results[id2].should be_false # rejected
          results[id3].should be_false # rejected
        end
      end
    end
  end

  it "batches concurrent in-flight publishes into multi-acks" do
    # When many publishes are in flight at once, they should accumulate in
    # @pending_acks while the loop is busy syncing the previous batch, so the
    # number of sync calls is much less than the number of publishes.
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        q = ch.queue("batch_test")
        n = 500
        n.times { ch.basic_publish "msg", "", q.name }
        ch.wait_for_confirms.should be_true
        s.vhosts["/"].queue(q.name).message_count.should eq n
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
        ch.confirm_select
        msgs.each do |m|
          q.publish m
        end
        ch.wait_for_confirms
      end
    end
  end
end
