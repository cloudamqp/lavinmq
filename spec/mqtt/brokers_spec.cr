require "./spec_helper"

describe LavinMQ::MQTT::Brokers do
  it "tolerates a Closed event for a vhost it never registered" do
    with_amqp_server do |s|
      mqtt_server = LavinMQ::MQTT::Server.new(s)
      begin
        brokers = mqtt_server.brokers
        # A vhost can be added to the store but never have its Added event fire
        # (e.g. its create's save! raised after adding it), so no MQTT broker
        # exists for it. A later Closed event for that vhost must not raise.
        count = brokers.@brokers.size
        brokers.on(LavinMQ::VHostStore::Event::Closed, "never-registered-vhost")
        brokers.@brokers.size.should eq count
      ensure
        mqtt_server.try &.close
      end
    end
  end
end
