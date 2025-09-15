require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe LavinMQ::MQTT do
    describe "permissions" do
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

      it "should enforce topic-specific write permissions" do
        with_server do |server|
          server.users.create("topic_user", "pass")
          server.users.add_permission("topic_user", "/", /.*/, /.*/, /allowed.*/) # config: .*, read: .*, write: allowed.*

          with_client_io(server) do |io|
            connect(io, username: "topic_user", password: "pass".to_slice)
            # This should fail and close connection
            publish(io, false, topic: "denied/topic", payload: "hello".to_slice)
            io.should be_closed
          end
        end
      end

      it "should enforce topic-specific read permissions" do
        with_server do |server|
          server.users.create("read_user", "pass")
          server.users.add_permission("read_user", "/", /.*/, /allowed.*/, /.*/) # config: .*, read: allowed.*, write: .*

          with_client_io(server) do |io|
            connect(io, username: "read_user", password: "pass".to_slice)
            # This should fail and close connection
            topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("denied/topic", 0u8)
            subscribe(io, false, topic_filters: [topic_filter])
            io.should be_closed
          end
        end
      end
    end
  end
end
