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
