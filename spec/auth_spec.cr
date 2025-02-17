require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/authenticators/basic"

describe LavinMQ::Auth::Chain do
  it "Creates a default authentication chain if not configured" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      chain.@backends.should be_a Array(LavinMQ::Auth::Authenticator)
      chain.@backends.size.should eq 1
    end
  end

  it "Successfully authenticates and returns a basic user" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      user = chain.authenticate("guest", "guest")
      user.should_not be_nil
    end
  end

  it "Does not authenticate when given invalid credentials" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      user = chain.authenticate("guest", "invalid")
      user.should be_nil
    end
  end

  it "Creates a authentication chain with basic and rabbit" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        auth_backends = http,basic
        auth_http_user_path = localhost:8081/auth/user
        [mgmt]
        [amqp]
      CONFIG
    end

    config = LavinMQ::Config.new
    config.config_file = config_file.path
    config.parse
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(config, s.@users)
      chain.@backends.should be_a Array(LavinMQ::Auth::Authenticator)
      chain.@backends.size.should eq 2
    end
  end
end
