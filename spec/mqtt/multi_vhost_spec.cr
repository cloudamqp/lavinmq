require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  describe LavinMQ::MQTT do
    describe "multi-vhost" do
      it "should create a broker when vhost is created" do
        with_server do |server|
          server.vhosts.create("new")
          server.mqtt_server.broker("new").vhost.should be server.vhosts["new"]
        end
      end

      it "should close the broker when vhost is deleted" do
        with_server do |server|
          broker = server.vhosts.create("new").mqtt_broker
          server.vhosts.delete("new")
          broker.@retain_store.@index_file.closed?.should be_true
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

        it "should deny mqtt access to a deleted vhost" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "new", /.*/, /.*/, /.*/
            server.vhosts.delete("new")
            # Permissions are dropped with the vhost, re-grant them so the
            # missing vhost is what refuses the connection
            server.users.add_permission "foo", "new", /.*/, /.*/, /.*/
            with_client_io(server) do |io|
              resp = connect io, username: "new:foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::NotAuthorized
            end
          end
        end

        it "should deny mqtt access to a closed vhost" do
          with_server do |server|
            server.vhosts.create("new")
            server.users.create("foo", "bar")
            server.users.add_permission "foo", "new", /.*/, /.*/, /.*/
            server.vhosts["new"].close
            with_client_io(server) do |io|
              resp = connect io, username: "new:foo", password: "bar".to_slice
              resp = resp.should be_a(MQTT::Protocol::Connack)
              resp.return_code.should eq MQTT::Protocol::Connack::ReturnCode::NotAuthorized
            end
          end
        end
      end
    end
  end
end
