require "./spec_helper"

describe "when disk is full" do
  before_each do
    system "mkdir -p /tmp/tmpfs"
  end

  after_each do
    system "umount /tmp/tmpfs 2> /dev/null"
  end

  it "should raise error on start server" do
    system("mount -t tmpfs -o size=500k lavinmq-spec /tmp/tmpfs") || pending! "Root required for tmpfs"
    expect_raises(File::Error) do
      LavinMQ::Server.new("/tmp/tmpfs")
    end
  end

  it "should raise error on publish" do
    system("mount -t tmpfs -o size=1m lavinmq-spec /tmp/tmpfs") || pending! "Root required for tmpfs"
    s = LavinMQ::Server.new("/tmp/tmpfs")
    spawn { s.listen("127.0.0.2", 5672) }
    message = "m"
    with_channel(host: "127.0.0.2") do |ch|
      q = ch.queue("queue")
      expect_raises(AMQP::Client::Error) do
        q.publish message * 134_217_728
      end
    end
    s.close
  end

  pending "should raise error on close server" do
    expect_raises(File::Error) do
    end
  end
end
