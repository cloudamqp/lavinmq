require "./spec_helper"
require "amqp-client"

describe LavinMQ::AMQP::Channel do
  describe "Configurations" do
    it "should respect consumer_max_per_channel" do
      config = LavinMQ::Config.new
      config.consumer_max_per_channel = 1
      with_amqp_server(config: config) do |s|
        puts "Creating resources"
        connection = AMQP::Client.new(port: amqp_port(s)).connect
        channel = connection.channel
        channel.queue("test:queue:1")
        channel.queue("test:queue:2")
        channel.basic_consume("test:queue:1") {}
        puts "Consuming Q1"
        begin
          channel.basic_consume("test:queue:2") {}
          fail "Expected NOT_ALLOWED error when trying to surpass consumer_max_per_channel"
        rescue ex : Exception 
          ex.should be_a(AMQP::Client::Channel::ClosedException)
        end
      end
    end
  end
end
