require "./spec_helper"
require "../src/lavinmq/auth/authenticators/local"

describe LavinMQ::Auth::LocalAuthenticator do
  describe "EXTERNAL mechanism" do
    it "authenticates an existing user without verifying a password" do
      with_amqp_server do |s|
        s.@users.create("anders", "irrelevant-password")
        authenticator = LavinMQ::Auth::LocalAuthenticator.new(s.@users)
        ctx = LavinMQ::Auth::Context.new("anders", Bytes.empty, mechanism: "EXTERNAL")
        authenticator.authenticate(ctx).should_not be_nil
      end
    end

    it "returns nil for an unknown user" do
      with_amqp_server do |s|
        authenticator = LavinMQ::Auth::LocalAuthenticator.new(s.@users)
        ctx = LavinMQ::Auth::Context.new("ghost", Bytes.empty, mechanism: "EXTERNAL")
        authenticator.authenticate(ctx).should be_nil
      end
    end
  end

  describe "PLAIN mechanism" do
    it "authenticates with the correct password" do
      with_amqp_server do |s|
        s.@users.create("anders", "secret")
        authenticator = LavinMQ::Auth::LocalAuthenticator.new(s.@users)
        ctx = LavinMQ::Auth::Context.new("anders", "secret".to_slice, mechanism: "PLAIN")
        authenticator.authenticate(ctx).should_not be_nil
      end
    end

    it "rejects a wrong password" do
      with_amqp_server do |s|
        s.@users.create("anders", "secret")
        authenticator = LavinMQ::Auth::LocalAuthenticator.new(s.@users)
        ctx = LavinMQ::Auth::Context.new("anders", "wrong".to_slice, mechanism: "PLAIN")
        authenticator.authenticate(ctx).should be_nil
      end
    end
  end
end
