require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe LavinMQ::MQTT do
    describe "permissions" do
      before_each do
        LavinMQ::Config.instance.mqtt_permission_check_enabled = true
      end

      it "should block publish when user has no write permissions" do
        with_server do |server|
          server.users.create("no_write", "pass")
          server.users.add_permission("no_write", "/", /.*/, /.*/, /^$/) # config: .*, read: .*, write: ^$

          with_client_io(server) do |io|
            connect(io, username: "no_write", password: "pass".to_slice)
            publish(io, false, topic: "test/topic", payload: "hello".to_slice)
            # Connection should be closed due to permission denial
            io.should be_closed
          end
        end
      end

      it "should block subscribe when user has no read permissions" do
        with_server do |server|
          server.users.create("no_read", "pass")
          server.users.add_permission("no_read", "/", /.*/, /^$/, /.*/) # config: .*, read: ^$, write: .*

          with_client_io(server) do |io|
            connect(io, username: "no_read", password: "pass".to_slice)
            topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 0u8)
            subscribe(io, false, topic_filters: [topic_filter])
            # Connection should be closed due to permission denial
            io.should be_closed
          end
        end
      end

      it "should allow operations when user has full permissions" do
        with_server do |server|
          server.users.create("full_user", "pass")
          server.users.add_permission("full_user", "/", /.*/, /.*/, /.*/) # full permissions

          with_client_io(server) do |io|
            connect(io, username: "full_user", password: "pass".to_slice)

            # Both operations should succeed
            publish(io, topic: "test/topic", payload: "hello".to_slice)
            topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 0u8)
            subscribe(io, topic_filters: [topic_filter])

            # Connection should still be alive
            io.should_not be_closed
          end
        end
      end

      it "should block will messages when user lacks write permissions" do
        with_server do |server|
          server.users.create("no_write", "pass")
          server.users.add_permission("no_write", "/", /.*/, /.*/, /^$/) # config: .*, read: .*, write: ^$

          with_client_io(server) do |subscriber_io|
            connect(subscriber_io, username: "guest", password: "guest".to_slice)
            initial_publish_count = server.@mqtt_brokers.@brokers["/"].@exchange.@publish_in_count.get

            # Create a client with no write permissions and a will message
            will_socket = nil
            with_client_io(server) do |will_client_io|
              will = MQTT::Protocol::Will.new(topic: "will/topic", payload: "dead".to_slice, qos: 0u8, retain: false)
              connect(will_client_io, username: "no_write", password: "pass".to_slice, will: will)
              will_socket = will_client_io.@io.as(TCPSocket)
              will_socket.close
            end

            sleep 0.1.seconds
            final_publish_count = server.@mqtt_brokers.@brokers["/"].@exchange.@publish_in_count.get
            final_publish_count.should eq(initial_publish_count)
          end
        end
      end
    end
  end
end
