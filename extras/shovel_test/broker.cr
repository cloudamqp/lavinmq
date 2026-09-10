require "amqp-client"
require "./config"

module ShovelTest
  # The AMQP side: publishing into Source queues.
  class Broker
    class Error < Exception; end

    def initialize(@cfg : Config)
    end

    # Publishes `count` messages to `queue` through the default exchange and
    # waits for the broker to confirm all of them.
    def publish(queue : String, count : Int, body : String = "msg", content_type : String? = nil) : Nil
      AMQP::Client.start(@cfg.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        props = AMQP::Client::Properties.new(content_type: content_type)
        count.times { |i| ch.basic_publish("#{body} #{i}", "", queue, props: props) }
        ch.wait_for_confirms || raise Error.new("publish to #{queue} was nacked")
      end
    end
  end
end
