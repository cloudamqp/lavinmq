require "../spec/spec_helper"

# To run these specs you need to create a small RAM-disk, see https://gist.github.com/htr3n/344f06ba2bb20b1056d7d5570fe7f596.

describe "when disk is full" do
  it "should raise error on start server" do
    expect_raises(File::Error) do
      TestHelpers.create_servers("/Volumes/TinyDisk-0.5MB")
    end
  end

  it "should raise error on publish" do
    close_servers
    TestHelpers.create_servers("/Volumes/TinyDisk-1MB")
    message = "m"
    with_channel do |ch|
      q = ch.queue("queue")
      expect_raises(AMQP::Client::Error) do
        q.publish message * 134_217_728
      end
    end
  end

  it "should raise error on close server" do
    expect_raises(File::Error) do
      close_servers
    end
  end
end
