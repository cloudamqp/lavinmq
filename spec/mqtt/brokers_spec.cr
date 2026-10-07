require "./spec_helper"

describe LavinMQ::MQTT::Brokers do
  it "tracks vhost creation, deletion, and shutdown" do
    with_amqp_server do |s|
      brokers = s.mqtt_server.brokers
      brokers.broker("/").vhost.should eq s.vhosts["/"]

      vhost = s.vhosts.create("mqtt-lifecycle")
      brokers.broker(vhost.name).vhost.should eq vhost
      s.vhosts.delete(vhost.name)
      brokers[vhost.name]?.should be_nil

      replacement = s.vhosts.create(vhost.name)
      brokers.broker(replacement.name).vhost.should eq replacement
      s.vhosts.close
      brokers.@brokers.empty?.should be_true
    end
  end

  it "tolerates closing a vhost it never registered" do
    with_amqp_server do |s|
      brokers = s.mqtt_server.brokers
      # A failed save during vhost creation can leave a vhost without an MQTT
      # broker. Closing that vhost must not raise.
      count = brokers.@brokers.size
      brokers.close("never-registered-vhost")
      brokers.@brokers.size.should eq count
    end
  end

  it "ignores vhost changes after closing" do
    with_amqp_server do |s|
      brokers = s.mqtt_server.brokers
      brokers.close
      brokers.close

      vhost = s.vhosts.create("late-vhost")
      brokers.create(vhost)
      brokers.delete(vhost.name)
      brokers.close(vhost.name)
      brokers.@brokers.empty?.should be_true
    end
  end
end
