require "./spec_helper"
require "./../src/lavinmq/queue"

describe "Dead lettering" do
  q_name = "ttl"
  q_name_delayed = "ttl_delayed"
  q_name_delayed_2 = "ttl_delayed_2"

  # Verifies bugfix for Sub-table memory corruption in amq-protocol.cr
  # https://github.com/cloudamqp/amq-protocol.cr/pull/14
  it "should be able to read messages that has been dead lettered multiple times" do
    with_channel do |ch|
      q_delayed_2 = ch.queue(q_name_delayed_2, args: AMQP::Client::Arguments.new(
        {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name_delayed}
      ))
      q_delayed = ch.queue(q_name_delayed, args: AMQP::Client::Arguments.new(
        {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => q_name}
      ))
      q = ch.queue(q_name)

      x = ch.default_exchange
      x.publish_confirm("ttl", q_delayed_2.name)
      msg = wait_for { q.get }

      x_death = msg.properties.headers.not_nil!["x-death"].as(Array(AMQ::Protocol::Field))
      x_death.inspect
      x_death.size.should eq 2
      x_death[0].as(AMQ::Protocol::Table)["queue"].should eq q_delayed.name
      x_death[1].as(AMQ::Protocol::Table)["queue"].should eq q_delayed_2.name
    end
  end
end
