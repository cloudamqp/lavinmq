require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  describe LavinMQ::MQTT do
    describe "multi-vhost" do
      it "should create mqtt exchange when vhost is created" do
        with_server do |server|
          server.vhosts.create("new")
          server.vhosts["new"].exchange?(LavinMQ::MQTT::EXCHANGE).should_not be_nil
        end
      end

      it "closes broker with retained messages when vhost is deleted" do
        with_server do |server|
          vhost = "retained"
          server.vhosts.create(vhost)
          broker = mqtt(server).broker(vhost)
          broker.publish(MQTT::Protocol::Publish.new(
            topic: "test/retain",
            payload: "retained".to_slice,
            packet_id: nil,
            dup: false,
            qos: 0u8,
            retain: true
          ))

          server.vhosts.delete(vhost)

          expect_raises(KeyError) { mqtt(server).broker(vhost) }
        end
      end

      describe "authentication" do
        it "should deny mqtt access to default vhost for user lacking vhost permissions" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "new", /.*/, /.*/, /.*/
            with_client_io(server) do |io|
              resp = connect io, username: "foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::NotAuthorized
            end
          end
        end

        it "should allow mqtt access to default vhost for user with vhost permissions" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "/", /.*/, /.*/, /.*/
            with_client_io(server) do |io|
              resp = connect io, username: "foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            end
          end
        end

        it "should deny mqtt access to non-default vhost for user lacking vhost permissions" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "/", /.*/, /.*/, /.*/
            with_client_io(server) do |io|
              resp = connect io, username: "new:foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::NotAuthorized
            end
          end
        end

        it "should allow mqtt access to non-default vhost for user with vhost permissions" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "new", /.*/, /.*/, /.*/
            with_client_io(server) do |io|
              resp = connect io, username: "new:foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            end
          end
        end
      end
    end
  end
end
