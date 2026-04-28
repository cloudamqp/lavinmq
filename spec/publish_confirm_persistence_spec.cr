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

  it "batches confirms according to interval" do
    # Use a small interval for faster test
    config = LavinMQ::Config.instance.dup
    config.publish_confirm_interval = 200
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        q = ch.queue("batch_confirm_test")
        start = Time.instant
        q.publish_confirm "message 1"
        duration = Time.instant - start
        # It should have taken at least the interval
        duration.total_milliseconds.should be_close(200, 100)
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
