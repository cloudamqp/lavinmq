require "../spec/spec_helper"

# These specs rely on specific volumes being mounted and size limited
# Must be run via Docker (compose)
describe "when disk is full" do
  before_all do
    # Start with servers stopped
    close_servers
  end

  before_each do
    pending!("Must be run in container test environment") unless ENV["SPEC_CONTAINER"]?
  end

  after_each do
    pp `df -h`
  end

  it "should raise error on start server" do
    expect_raises(File::Error) do
      TestHelpers.create_servers("/16K")
    end
  end

  it "should raise error on publish" do
    close_servers
    TestHelpers.create_servers("/1MB")
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
