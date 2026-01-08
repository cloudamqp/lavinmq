require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/authenticators/local"

describe LavinMQ::Auth::Chain do
  it "creates a default authentication chain if not configured" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@users)
      chain.@backends.should be_a Array(LavinMQ::Auth::Authenticator)
      chain.@backends.size.should eq 1
    end
  end

  it "successfully authenticates and returns a local user" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@users)
      ctx = LavinMQ::Auth::Context.new("guest", "guest".to_slice, loopback: true)
      user = chain.authenticate(ctx)
      user.should_not be_nil
    end
  end

  it "does not authenticate when given invalid credentials" do
    with_amqp_server do |s|
      s.@users.create("foo", "bar")
      chain = LavinMQ::Auth::Chain.create(s.@users)
      ctx = LavinMQ::Auth::Context.new("bar", "baz".to_slice, loopback: true)
      user = chain.authenticate(ctx)
      user.should be_nil
    end
  end

  it "requires loopback if Config.#default_user_only_loopback? is true" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.default_user_only_loopback = true
      chain = LavinMQ::Auth::Chain.create(s.@users)
      ctx = LavinMQ::Auth::Context.new("guest", "guest".to_slice, loopback: false)
      user = chain.authenticate(ctx)
      user.should be_nil
    end
  end
end
