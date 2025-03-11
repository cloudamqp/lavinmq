require "./spec_helper"
require "../src/lavinmq/amqp"

describe LavinMQ::DeletedVhostCounters do
  it "tracks global counters correctly after vhost deletion" do
    with_amqp_server do |server|
      server.vhosts.create("test")

      with_channel(server, vhost: "test") do |ch|
        q = ch.queue("test_queue", durable: false)
        q.publish("msg")

        delivery = q.get(no_ack: false)
        delivery.ack if delivery
      end

      server.update_deleted_vhost_counters(server.vhosts["test"])
      server.deleted_vhost_messages_acknowledged_total.should eq 1
    end
  end
end
