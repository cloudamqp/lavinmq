require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/authenticators/basic"
require "../src/lavinmq/user_store"

describe LavinMQ::Auth::Chain do

  it "Creates a default authentication chain if not configured" do
     with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@users)

      chain.@backends.is_a?(Array(LavinMQ::Auth::Authenticator)).should be_true
      chain.@backends.size.should eq 1
     end
  end

  it "Successfully authenticates and returns a basic user" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@users)
      user = chain.authenticate("guest", "guest")
      user.should_not be_nil
    end
  end

  it "Does not authenticate when given invalid credentials" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@users)
      user = chain.authenticate("guest", "invalid")
      user.should be_nil
    end
  end
end
