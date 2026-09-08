require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT client_id validation" do
    after_each do
      LavinMQ::Config.instance.mqtt_client_id_validation = LavinMQ::MQTT::ClientIdValidation::None
    end

    describe "username mode" do
      before_each do
        LavinMQ::Config.instance.mqtt_client_id_validation = LavinMQ::MQTT::ClientIdValidation::Username
      end

      it "accepts a client_id equal to the username" do
        with_server do |server|
          with_client_io(server) do |io|
            connack = connect(io, client_id: "guest")
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          end
        end
      end

      it "rejects a client_id that differs from the username" do
        with_server do |server|
          with_client_io(server) do |io|
            connack = connect(io, client_id: "not_guest")
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::IdentifierRejected
            io.should be_closed
          end
        end
      end

      it "assigns the username as client_id when it is empty" do
        with_server do |server|
          with_client_io(server) do |io|
            connack = connect(io, client_id: "", clean_session: true)
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            # Proof the assigned id is the username: a second connection with
            # client_id "guest" must take over this session [MQTT-3.1.4-2]
            with_client_io(server) do |io2|
              connect(io2, client_id: "guest")
              io.should be_closed
            end
          end
        end
      end

      it "validates against the username without the vhost prefix" do
        with_server do |server|
          with_client_io(server) do |io|
            connack = connect(io, username: "/:guest", client_id: "guest")
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          end
        end
      end

      it "assigns the username as client_id on v5 and echoes it back" do
        with_server do |server|
          with_client_socket(server) do |socket|
            io = MQTT::Protocol::IO::V5.new(socket)
            connack = connect(io, version: MQTT::Protocol::Version::V5,
              client_id: "", clean_session: true).as(MQTT::Protocol::Connack)
            connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            # The assigned id must be the username, and it must survive the
            # rebuild of the CONNECT packet intact so the broker registers it.
            connack.properties.assigned_client_identifier.should eq("guest")
            registered = wait_for do
              server.vhosts["/"].connections.select(LavinMQ::MQTT::Client).first?.try(&.client_id)
            end
            registered.should eq("guest")
          end
        end
      end
    end

    describe "none mode (default)" do
      it "accepts any client_id" do
        with_server do |server|
          with_client_io(server) do |io|
            connack = connect(io, client_id: "anything_goes")
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          end
        end
      end
    end
  end
end
