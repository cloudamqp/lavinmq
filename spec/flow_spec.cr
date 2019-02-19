require "./spec_helper"
require "benchmark"

describe "Flow" do
  it "should support consumer flow" do
    with_channel do |ch|
      q = ch.queue
      q.publish "test"
      ch.flow(false)
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      sleep 0.05
      msgs.size.should eq 0
    end
  end

  it "should support server flow" do
    s.flow(false)
    with_channel do |ch|
      q = ch.queue
      Benchmark.realtime { q.publish_confirm("m1") }.milliseconds.should be > 50
    end
  ensure
    s.flow(true)
  end
end
