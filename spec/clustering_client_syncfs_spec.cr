require "./spec_helper"
require "../src/lavinmq/clustering/client"

private class TimeoutSyncClient < LavinMQ::Clustering::Client
  def wait_for_syncfs_public : Nil
    wait_for_syncfs
  end

  protected def syncfs_timeout : Time::Span
    1.millisecond
  end
end

describe LavinMQ::Clustering::Client do
  it "exits when syncfs is blocked past the timeout" do
    config = LavinMQ::Config.instance
    config.metrics_http_port = -1
    client = TimeoutSyncClient.new(config, 1, "secret", proxy: false)

    ex = expect_raises(SpecExit) { client.wait_for_syncfs_public }
    ex.code.should eq 1
  ensure
    client.try &.close
  end
end
