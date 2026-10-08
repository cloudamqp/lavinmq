require "../spec_helper"
require "log/spec"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "connect [MQTT-3.1.4-1]" do
    describe "when client already connected" do
      it "should replace the already connected client [MQTT-3.1.4-3]" do
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

      it "keeps the replacement client registered after the old client cleanup runs" do
        with_server do |server|
          client_id = "reconnect-race"
          broker = server.mqtt_server.broker("/")
          vhost = server.vhosts["/"]

          with_client_io(server) do |io|
            connect(io, client_id: client_id)
            old_client = broker.@clients[client_id]

            with_client_io(server) do |io2|
              connect(io2, client_id: client_id)
              new_client = wait_for do
                c = broker.@clients[client_id]?
                c if c && !c.same?(old_client)
              end.not_nil!

              io.should be_closed
              wait_for { vhost.connections.none?(&.same?(old_client)) }

              broker.@clients[client_id]?.try(&.same?(new_client)).should be_true
              pingpong(io2).should be_a(MQTT::Protocol::PingResp)
            end
          end
        end
      end
    end

    describe "receives connack" do
      describe "with expected flags set" do
        it "no session present when reconnecting a non-clean session with a clean session [MQTT-3.2.2-2]" do
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
              connack = connect(io, clean_session: true)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.session_present?.should be_false
            end
          end
        end

        it "no session present when reconnecting a clean session with a non-clean session [MQTT-3.2.2-3]" do
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

        it "no session present when reconnecting a clean session [MQTT-3.2.2-2]" do
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

        it "session present when reconnecting a non-clean session [MQTT-3.2.2-3]" do
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

        it "session present when reconnecting a non-clean session without subscriptions [MQTT-3.2.2-3]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: false)
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

        it "closes the connection when its session is deleted" do
          with_server do |server|
            vhost = server.vhosts["/"]
            with_client_io(server) do |io|
              connect(io, client_id: "a", clean_session: false)
              # A deliberate delete, so the read fiber must not log an error.
              Log.capture("lmq.mqtt.client", :error) do |logs|
                vhost.delete_queue("mqtt.a")
                io.should be_closed
                wait_for { vhost.@connections.@connections.empty? }
                logs.empty
              end
            end
            with_client_io(server) do |io|
              connack = connect(io, client_id: "a", clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              connack.as(MQTT::Protocol::Connack).session_present?.should be_false
            end
          end
        end

        it "keeps a reconnect to a session closed by a store error, until the session is deleted" do
          with_server do |server|
            vhost = server.vhosts["/"]
            with_client_io(server) do |io|
              connect(io, client_id: "a", clean_session: false)
              # Stands in for `get_packet` closing the session on a
              # MessageStore::Error while this client is attached.
              vhost.session("mqtt.a").close
              disconnect(io)
            end
            wait_for { vhost.@connections.@connections.empty? }

            with_client_io(server) do |io|
              connack = connect(io, client_id: "a", clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              pingpong(io).should be_a(MQTT::Protocol::PingResp)
              vhost.delete_queue("mqtt.a")
              io.should be_closed
            end
          end
        end

        it "no session present when taking over a session that ends with its connection" do
          # The previous connection's 0-interval session is still there when the
          # takeover starts, but the takeover ends it, so the new connection must
          # not be told it resumed one, and must not inherit the subscription.
          with_server do |server|
            with_client_io(server) do |first|
              connect(first, clean_session: true)
              subscribe(first,
                topic_filters: [subtopic("a/topic", 0u8)],
                packet_id: 1u16
              )

              with_client_io(server) do |second|
                connack = connect(second, clean_session: false).as(MQTT::Protocol::Connack)
                connack.session_present?.should be_false
                # The CONNACK is written before add_client runs; a PINGREQ
                # round-trip proves the takeover has been applied.
                pingpong(second)
                # Every connection gets a session, so this one has a fresh durable
                # session, without the subscription the old one held.
                vhost = server.vhosts["/"]
                vhost.session("mqtt.client_id").durable?.should be_true
                vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange).bindings_details.should be_empty
                disconnect(second)
              end
            end
          end
        end
      end

      describe "with expected return code" do
        it "for valid credentials [MQTT-3.2.0-1]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
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
        #       connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::NotAuthorized)
        #       # Verify that connection is closed [MQTT-3.1.4-1]
        #       io.should be_closed
        #     end
        #   end
        # end

        it "for invalid protocol version [MQTT-3.1.2-2]" do
          with_server do |server|
            with_client_io(server) do |io|
              temp_io = IO::Memory.new
              temp_mqtt_io = MQTT::Protocol::IO.v3(temp_io)
              connect(temp_mqtt_io, expect_response: false)
              temp_io.rewind
              connect_pkt = temp_io.to_slice
              # This will overwrite the protocol level byte
              connect_pkt[8] = 9u8
              io.write_bytes_raw connect_pkt

              connack = MQTT::Protocol::Packet.from_io(io)

              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::UnsupportedProtocolVersion)
              # Verify that connection is closed [MQTT-3.1.4-1]
              io.should be_closed
            end
          end
        end

        it "client_id must be the first field of the connect packet [MQTT-3.1.3-3]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                "client_id",
                clean_start: true,
                keep_alive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                version: MQTT::Protocol::Version::V3_1_1,
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

        it "accepts zero-byte ClientId with CleanSession set to 1 [MQTT-3.1.3-7 v3.1.1]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io, client_id: "", clean_session: true)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
              io.should_not be_closed
            end
          end
        end

        it "for a client id whose session name is taken by an AMQP queue" do
          with_server do |server|
            # What a definitions import of a plain `mqtt.a` queue creates; AMQP
            # and the HTTP API refuse the prefix.
            server.vhosts["/"].declare_queue("mqtt.a", true, false)
            with_client_io(server) do |io|
              connack = connect(io, client_id: "a")
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::ClientIdentifierNotValid)
              io.should be_closed
            end
          end
        end

        it "for empty client id with non-clean session [MQTT-3.1.3-8 v3.1.1]" do
          with_server do |server|
            with_client_io(server) do |io|
              connack = connect(io, client_id: "", clean_session: false)
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::ClientIdentifierNotValid)
              io.should be_closed
            end
          end
        end

        it "for password flag set without username flag set [MQTT-3.1.2-22 v3.1.1]" do
          with_server do |server|
            with_client_io(server) do |io|
              # The shard forbids constructing a v3 password-without-username
              # CONNECT, so craft the malformed packet: build a valid
              # username+password CONNECT and clear the username flag (bit 7),
              # leaving the password flag set.
              connect = MQTT::Protocol::Connect.new(
                "client_id",
                clean_start: true,
                keep_alive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                version: MQTT::Protocol::Version::V3_1_1,
              ).to_slice
              connect[9] &= 0b0111_1111
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

        it "for invalid client id [MQTT-1.5.4-2]" do
          with_server do |server|
            with_client_io(server) do |io|
              MQTT::Protocol::Connect.new(
                "client\u0000_id",
                clean_start: true,
                keep_alive: 30u16,
                username: "valid_user",
                password: "valid_user".to_slice,
                version: MQTT::Protocol::Version::V3_1_1,
              ).to_io(io)

              io.should be_closed
            end
          end
        end

        it "for invalid protocol name [MQTT-3.1.2-1]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                "client_id",
                clean_start: true,
                keep_alive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                version: MQTT::Protocol::Version::V3_1_1,
              ).to_slice

              # This will overwrite the last "T" in MQTT
              connect[7] = 'x'.ord.to_u8
              io.write_bytes_raw connect

              packet = MQTT::Protocol::Packet.from_io(io)
              packet.should be_a(MQTT::Protocol::Connack)
              packet.as(MQTT::Protocol::Connack).reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::UnsupportedProtocolVersion)
              io.should be_closed
            end
          end
        end

        it "for reserved bit set [MQTT-3.1.2-3]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect = MQTT::Protocol::Connect.new(
                "client_id",
                clean_start: true,
                keep_alive: 30u16,
                username: "valid_user",
                password: "valid_password".to_slice,
                version: MQTT::Protocol::Version::V3_1_1,
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
            server.vhosts["/"].session("mqtt.client_id").consumer_count.should eq 0
          end
        end
      end
    end
  end

  describe "MQTT 5.0 unexpected packets" do
    it "answers DISCONNECT ProtocolError (0x82) to a second CONNECT [MQTT-3.1.0-2]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket)
          connect(io, expect_response: false, version: MQTT::Protocol::Version::V5)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end
  end

  describe "MQTT 5.0 connect" do
    it "negotiates the protocol version from CONNECT and replies with a v5 CONNACK" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          # A v5 CONNECT must be answered with a v5-framed CONNACK; if the
          # broker kept v3 framing the reply would be unparseable here.
          connack = connect(io, version: MQTT::Protocol::Version::V5)
          connack.should be_a(MQTT::Protocol::Connack)
          connack = connack.as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
        end
      end
    end

    it "closes without a CONNACK that exceeds the client's Maximum Packet Size [MQTT-3.1.2-24]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          props = MQTT::Protocol::ConnectProperties.new
          # Our CONNACK carries the capability set, so it is well over 5 bytes.
          props.maximum_packet_size = 5u32
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "tiny",
            properties: props, expect_response: false)
          io.should be_closed
          # Refused before `run_client`, so no session was made either.
          server.vhosts["/"].session?("mqtt.tiny").should be_nil
        end
      end
    end

    it "echoes a server-assigned client id via assigned_client_identifier [MQTT-3.2.2-16]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "", clean_session: true).as(MQTT::Protocol::Connack)
          assigned = connack.properties.assigned_client_identifier
          assigned.should_not be_nil
          assigned = assigned.not_nil!
          assigned.should_not be_empty
          # The advertised id must be the one the broker actually registered.
          registered = wait_for do
            server.vhosts["/"].connections.select(LavinMQ::MQTT::Client).first?.try(&.client_id)
          end
          registered.should eq(assigned)
        end
      end
    end

    it "assigns a client id when Clean Start is 0 too (§3.1.3.1)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          # v3.1.1 rejects an empty client id without Clean Session
          # [MQTT-3.1.3-8 v3.1.1]; v5 dropped that condition.
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "", clean_session: false).as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
          connack.properties.assigned_client_identifier.should_not be_nil
        end
      end
    end

    it "does not set assigned_client_identifier when the client supplies a client id" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "supplied-id").as(MQTT::Protocol::Connack)
          connack.properties.assigned_client_identifier.should be_nil
        end
      end
    end

    it "sends DISCONNECT SessionTakenOver (0x8E) to the connection that loses a takeover [MQTT-3.1.4-3]" do
      with_server do |server|
        with_client_socket(server) do |first_socket|
          first = v5_connect(first_socket, client_id: "taken")
          with_client_socket(server) do |second_socket|
            v5_connect(second_socket, client_id: "taken")

            pkt = MQTT::Protocol::Packet.from_io(first)
            pkt.should be_a(MQTT::Protocol::Disconnect)
            pkt.as(MQTT::Protocol::Disconnect).reason_code
              .should eq(MQTT::Protocol::Disconnect::ReasonCode::SessionTakenOver)
            first.should be_closed
          end
        end
      end
    end

    it "takes over a connection whose delivery is blocked on a full socket" do
      with_server do |server|
        with_client_socket(server) do |stalled_socket|
          stalled_socket.recv_buffer_size = 4096
          stalled = v5_connect(stalled_socket, client_id: "stalled")
          subscribe(stalled, topic_filters: [subtopic("big", 0u8)])
          with_client_io(server) do |publisher|
            connect(publisher, client_id: "publisher")
            payload = Bytes.new(256 * 1024)
            40.times { publish(publisher, topic: "big", payload: payload, qos: 0u8) }
            pingpong(publisher)
          end
          # The stalled client never reads, so its delivery fiber blocks in a
          # write while holding the connection's write lock.
          sleep 0.5.seconds

          with_client_socket(server) do |socket|
            socket.read_timeout = 5.seconds
            io = v5_connect(socket, client_id: "stalled")
            # The CONNACK precedes the takeover; a PINGREQ proves it completed.
            pingpong(io).should be_a(MQTT::Protocol::PingResp)
          end
        end
      end
    end
  end
end
