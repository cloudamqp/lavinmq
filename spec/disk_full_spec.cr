require "./spec_helper"

describe "when disk is full" do
  before_each do
    close_servers
    system "mkdir -p /tmp/tmpfs"
  end

  after_each do
    close_servers
    system "umount /tmp/tmpfs"
  end

  it "should raise error on start server" do
    system("mount -t tmpfs -o size=500k lavinmq-spec /tmp/tmpfs") || pending! "Root required for tmpfs"
    expect_raises(File::Error) do
      TestHelpers.create_servers("/tmp/tmpfs")
    end
  end

  it "should raise error on publish" do
    system("mount -t tmpfs -o size=1m lavinmq-spec /tmp/tmpfs") || pending! "Root required for tmpfs"
    TestHelpers.create_servers("/tmp/tmpfs")
    message = "m"
    with_channel do |ch|
      q = ch.queue("queue")
      expect_raises(AMQP::Client::Error) do
        q.publish message * 134_217_728
      end
    end
  end

  pending "should raise error on close server" do
    expect_raises(File::Error) do
      close_servers
    end
  end
end
