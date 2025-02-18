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
        rabbit_backend_url = localhost:8081
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

describe AuthBackend do
  it "respond allow with guest credential" do
    ab = AuthBackend.new
    spawn(name: "test auth backend") { ab.listen }
    payload = {
      "username" => "guest",
      "password" => "http_secret",
    }.to_json

    response = ::HTTP::Client.post("localhost:8081/auth/user",
      headers: ::HTTP::Headers{"Content-Type" => "application/json"},
      body: payload)
    response.body.should eq "allow"
    ab.close
  end
end

describe LavinMQ::Auth::RabbitBackendAuthenticator do
  it "Successfully authenticates guest user and return a basic user" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        auth_backends = http,basic
        rabbit_backend_url = localhost:8081
        [mgmt]
        [amqp]
      CONFIG
    end

    config = LavinMQ::Config.new
    config.config_file = config_file.path
    config.parse
    with_amqp_server do |s|
      with_http_auth_backend do |_|
        chain = LavinMQ::Auth::Chain.create(config, s.@users)
        user = chain.authenticate("guest", "http_secret")
        user.should_not be_nil
      end
    end
  end
end

describe LavinMQ::Auth::RabbitBackendClient do
  it "Successfully authenticate with guest credential" do
    with_http_auth_backend do |_|
      client = LavinMQ::Auth::RabbitBackendClient.new("localhost:8081")
      client.authenticate?("guest", "http_secret").should be_true
    end
  end
end
