require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "connect [MQTT-3.1.4-1]" do
    describe "when client already connected" do
      it "should replace the already connected client [MQTT-3.1.4-2]" do
        with_server do |server|
          with_client_io(server) do |io|
            connect(io)
            with_client_io(server) do |io2|
              connect(io2)
              io.should be_closed
            end
          end
        end
      end
    end

    describe "receives connack" do
      describe "with expected flags set" do
        it "no session present when reconnecting a non-clean session with a clean session [MQTT-3.1.2-6]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: false)

              # LavinMQ won't save sessions without subscriptions
              subscribe(io,
                topic_filters: [subtopic("a/topic", 0u8)],
                packet_id: 1u16
              )
              disconnect(io)
            end
            with_client_io(server) do |io|
              connack = connect(io, clean_session: true)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.session_present?.should be_false
            end
          end
        end

        it "no session present when reconnecting a clean session with a non-clean session [MQTT-3.1.2-6]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: true)
              subscribe(io,
                topic_filters: [subtopic("a/topic", 0u8)],
                packet_id: 1u16
              )
              disconnect(io)
            end
            with_client_io(server) do |io|
              connack = connect(io, clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.session_present?.should be_false
            end
          end
        end

        it "no session present when reconnecting a clean session [MQTT-3.1.2-6]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: true)
              subscribe(io,
                topic_filters: [subtopic("a/topic", 0u8)],
                packet_id: 1u16
              )
              disconnect(io)
            end
            with_client_io(server) do |io|
              connack = connect(io, clean_session: true)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.session_present?.should be_false
            end
          end
        end

        it "session present when reconnecting a non-clean session [MQTT-3.1.2-4]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: false)
              subscribe(io,
                topic_filters: [subtopic("a/topic", 0u8)],
                packet_id: 1u16
              )
              disconnect(io)
            end
            with_client_io(server) do |io|
              connack = connect(io, clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.session_present?.should be_true
            end
          end
        end
      end

      describe "with expected return code" do
        it "for valid credentials [MQTT-3.1.4-4]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::Accepted)
            end
          end
        end

        # pending "for invalid credentials" do
        #   auth = SpecAuth.new({"a" => {password: "b", acls: ["a", "a/b", "/", "/a"] of String}})
        #   with_server(auth: auth) do |server|
        #     with_client_io(server) do |io|
        #       connack = connect(io, username: "nouser")

        #       connack.should be_a(MQTT::Protocol::Connack)
        #       connack = connack.as(MQTT::Protocol::Connack)
        #       connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::NotAuthorized)
        #       # Verify that connection is closed [MQTT-3.1.4-1]
        #       io.should be_closed
        #     end
        #   end
        # end

        it "for invalid protocol version [MQTT-3.1.2-2]" do
          with_server do |server|
            with_client_io(server) do |io|
              temp_io = IO::Memory.new
              temp_mqtt_io = MQTT::Protocol::IO.new(temp_io)
              connect(temp_mqtt_io, expect_response: false)
              temp_io.rewind
              connect_pkt = temp_io.to_slice
              # This will overwrite the protocol level byte
              connect_pkt[8] = 9u8
              io.write_bytes_raw connect_pkt

              connack = MQTT::Protocol::Packet.from_io(io)

              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::UnacceptableProtocolVersion)
              # Verify that connection is closed [MQTT-3.1.4-1]
              io.should be_closed
            end
          end
        end

        it "client_id must be the first field of the connect packet [MQTT-3.1.3-3]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                client_id: "client_id",
                clean_session: true,
                keepalive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                will: nil
              ).to_slice
              connect[0] = 'x'.ord.to_u8
              io.write_bytes_raw connect
              io.should be_closed
            end
          end
        end

        it "accepts zero byte client_id but is assigned a unique client_id [MQTT-3.1.3-6]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, client_id: "", clean_session: true)
              server.vhosts["/"].connections.select(LavinMQ::MQTT::Client).first.client_id.should_not eq("")
            end
          end
        end

        it "accepts zero-byte ClientId with CleanSession set to 1 [MQTT-3.1.3-7]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io, client_id: "", clean_session: true)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::Accepted)
              io.should_not be_closed
            end
          end
        end

        it "for empty client id with non-clean session [MQTT-3.1.3-8]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io, client_id: "", clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::IdentifierRejected)
              io.should be_closed
            end
          end
        end

        it "for password flag set without username flag set [MQTT-3.1.2-22]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                client_id: "client_id",
                clean_session: true,
                keepalive: 30u16,
                username: nil,
                password: "valid_password".to_slice,
                will: nil
              ).to_slice
              # Set password flag
              connect[9] |= 0b0100_0000
              io.write_bytes_raw connect

              # Verify that connection is closed [MQTT-3.1.4-1]
              io.should be_closed
            end
          end
        end
      end

      describe "tcp socket is closed [MQTT-3.1.4-1]" do
        it "if first packet is not a CONNECT [MQTT-3.1.0-1]" do
          with_server do |server|
            with_client_io(server) do |io|
              payload = Bytes[1, 254, 200, 197, 123, 4, 87]
              publish(io, topic: "test", payload: payload, qos: 0u8)
              io.should be_closed
            end
          end
        end

        it "for a second CONNECT packet [MQTT-3.1.0-2]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io)
              connect(io, expect_response: false)

              io.should be_closed
            end
          end
        end

        it "for invalid client id [MQTT-3.1.3-4]." do
          with_server do |server|
            with_client_io(server) do |io|
              MQTT::Protocol::Connect.new(
                client_id: "client\u0000_id",
                clean_session: true,
                keepalive: 30u16,
                username: "valid_user",
                password: "valid_user".to_slice,
                will: nil
              ).to_io(io)

              io.should be_closed
            end
          end
        end

        it "for invalid protocol name [MQTT-3.1.2-1]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                client_id: "client_id",
                clean_session: true,
                keepalive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                will: nil
              ).to_slice

              # This will overwrite the last "T" in MQTT
              connect[7] = 'x'.ord.to_u8
              io.write_bytes_raw connect

              packet = MQTT::Protocol::Packet.from_io(io)
              packet.should be_a(MQTT::Protocol::Connack)
              packet.as(MQTT::Protocol::Connack).return_code.should eq(MQTT::Protocol::Connack::ReturnCode::UnacceptableProtocolVersion)
              io.should be_closed
            end
          end
        end

        it "for reserved bit set [MQTT-3.1.2-3]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                client_id: "client_id",
                clean_session: true,
                keepalive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                will: nil
              ).to_slice
              connect[9] |= 0b0000_0001
              io.write_bytes_raw connect

              io.should be_closed
            end
          end
        end

        it "should not publish after disconnect" do
          with_server do |server|
            # Create a non-clean session with an active subscription
            with_client_io(server) do |io|
              connect(io, clean_session: false)
              topics = mk_topic_filters({"a/b", 1})
              subscribe(io, topic_filters: topics)
              disconnect(io)
            end
            sleep 100.milliseconds
            server.vhosts["/"].queues["mqtt.client_id"].consumers.should be_empty
          end
        end
      end
    end
  end
end
