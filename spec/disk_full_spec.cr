require "./spec_helper"

describe LavinMQ::Server do
  before_each do
    system "mkdir -p /tmp/tmpfs"
  end

  after_each do
    system "umount /tmp/tmpfs 2> /dev/null"
  end

  it "will raise error on start server when out of disk" do
    system("mount -t tmpfs -o size=500k lavinmq-spec /tmp/tmpfs") || pending! "Root required for tmpfs"
    expect_raises(File::Error) do
      LavinMQ::Server.new("/tmp/tmpfs")
    end
  end

  it "will raise error on publish when out of disk" do
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

  pending "will raise error on close server when out of disk" do
    expect_raises(File::Error) do
    end
  end
end
