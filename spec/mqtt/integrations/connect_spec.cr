require "../spec_helper"
require "log/spec"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "connect [MQTT-3.1.4-1]" do
    # As when the vhost closes while the client connects, e.g. on shutdown:
    # the client is added once it's closed, before its read loop has started
    it "disconnects a client added to a closed vhost" do
      with_server do |server|
        vhost = server.vhosts["/"]
        broker = server.mqtt_server.broker("/")
        vhost.close
        client_end, server_end = UNIXSocket.pair
        begin
          packet = MQTT::Protocol::Connect.new(client_id: "late", clean_session: true, keepalive: 30u16,
            username: "guest", password: "guest".to_slice, will: nil)
          added = Channel(Nil).new
          spawn do
            broker.run_client(MQTT::Protocol::IO.new(server_end), LavinMQ::ConnectionInfo.local,
              server.users["guest"], packet) { }
            added.close
          end
          select
          when added.receive?
          when timeout(5.seconds)
            fail "adding the client hung"
          end
          client_end.read_timeout = 5.seconds
          MQTT::Protocol::IO.new(client_end).should be_closed
        ensure
          client_end.close
          server_end.close
        end
      end
    end

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
              end

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
        it "no session present when reconnecting a non-clean session with a clean session [MQTT-3.1.2-6]" do
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

        it "no session present for a persistent client that only published at QoS 0 or 1" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: false)
              publish(io, topic: "a/b", qos: 0u8)
              publish(io, topic: "a/b", qos: 1u8)
              disconnect(io)
            end
            with_client_io(server) do |io|
              # It held no state, and like on main its session never became a queue
              connect(io, clean_session: false).as(MQTT::Protocol::Connack).session_present?.should be_false
            end
          end
        end

        it "session present after a durable QoS 2 publish [MQTT-3.1.2-4]" do
          with_server do |server|
            with_client_io(server) do |io|
              connect(io, clean_session: false)
              publish(io, topic: "a/b", qos: 2u8, packet_id: 7u16)
              disconnect(io)
            end
            with_client_io(server) do |io|
              connect(io, clean_session: false).as(MQTT::Protocol::Connack).session_present?.should be_true
              # Still held, so a re-send is answered without routing it again
              publish(io, topic: "a/b", qos: 2u8, packet_id: 7u16, dup: true)
              pubrel(io, 7u16)
              read_packet(io).should be_a(MQTT::Protocol::PubComp)
            end
          end
        end

        it "closes the connection when its session is deleted" do
          with_server do |server|
            vhost = server.vhosts["/"]
            with_client_io(server) do |io|
              connect(io, client_id: "a", clean_session: false)
              subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}))
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

        it "refuses a reconnect to a session closed by a store error, until the session is deleted" do
          with_server do |server|
            vhost = server.vhosts["/"]
            with_client_io(server) do |io|
              connect(io, client_id: "a", clean_session: false)
              subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}))
              # Stands in for `get_packet` closing the session on a
              # MessageStore::Error while this client is attached.
              vhost.session("mqtt.a").close
              # Its QoS 2 ids are no longer recorded
              io.should be_closed
            end
            wait_for { vhost.@connections.@connections.empty? }

            # A closed session has no state to resume, and attaching to it
            # would get no deliveries
            with_client_io(server) do |io|
              connack = connect(io, client_id: "a", clean_session: false).as(MQTT::Protocol::Connack)
              connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::ServerUnavailable
            end
            vhost.delete_queue("mqtt.a")
            with_client_io(server) do |io|
              connack = connect(io, client_id: "a", clean_session: false).as(MQTT::Protocol::Connack)
              connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
              connack.session_present?.should be_false
              disconnect(io)
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

        it "for a client id whose session name is taken by an AMQP queue" do
          with_server do |server|
            # What a definitions import of a plain `mqtt.a` queue creates; AMQP
            # and the HTTP API refuse the prefix.
            server.vhosts["/"].declare_queue("mqtt.a", true, false)
            with_client_io(server) do |io|
              connack = connect(io, client_id: "a")
              connack.should be_a(MQTT::Protocol::Connack)
              connack = connack.as(MQTT::Protocol::Connack)
              connack.return_code.should eq(MQTT::Protocol::Connack::ReturnCode::IdentifierRejected)
              io.should be_closed
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
            server.vhosts["/"].session("mqtt.client_id").consumer_count.should eq 0
          end
        end
      end
    end
  end

  describe "a session's resources" do
    session_dir = ->(server : LavinMQ::Server, client_id : String) do
      File.join(server.vhosts["/"].data_dir, Digest::SHA1.hexdigest("mqtt.#{client_id}"))
    end
    session_files = ->(server : LavinMQ::Server, client_id : String) do
      dir = session_dir.call(server, client_id)
      Dir.exists?(dir) ? Dir.children(dir).select(&.starts_with?("msgs.")).sort! : Array(String).new
    end
    deliver_loop_started = ->(server : LavinMQ::Server, client_id : String) do
      server.vhosts["/"].session("mqtt.#{client_id}").@deliver_loop_started
    end

    {false, true}.each do |clean_session|
      it "creates no session queue for a publish-only client (clean_session=#{clean_session})" do
        with_server do |server|
          vhost = server.vhosts["/"]
          with_client_io(server) do |io|
            connect(io, client_id: "pub", clean_session: clean_session)
            publish(io, topic: "a/b", qos: 1u8, packet_id: 1u16)
            publish(io, topic: "a/b", qos: 0u8)
            # A clean session's QoS 2 state is in memory, so it needs no queue either
            publish(io, topic: "a/b", qos: 2u8, packet_id: 2u16) if clean_session
            pingpong(io)
            vhost.session?("mqtt.pub").should be_nil
            Dir.exists?(session_dir.call(server, "pub")).should be_false
            session = client_session(server, "pub")
            session.@msg_store.should be_nil
            session.@deliver_loop_started.should be_false
            disconnect(io)
          end
          vhost.sessions_size.should eq 0
        end
      end
    end

    it "creates the segment at SUBSCRIBE and starts delivering on the first message" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          session_files.call(server, "sub").should be_empty
          subscribe(sub, topic_filters: mk_topic_filters({"a/b", 1u8}))
          session_files.call(server, "sub").should eq ["msgs.0000000001"]
          deliver_loop_started.call(server, "sub").should be_false
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            publish(pub, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 2u16)
            publish(pub, topic: "a/b", payload: "2".to_slice, qos: 1u8, packet_id: 3u16)
            disconnect(pub)
          end
          {read_publish(sub), read_publish(sub)}.map { |p| String.new(p.payload) }.should eq({"1", "2"})
          session_files.call(server, "sub").should eq ["msgs.0000000001"]
          deliver_loop_started.call(server, "sub").should be_true
          disconnect(sub)
        end
      end
    end

    it "delivers what an offline session got before its first connection back" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          subscribe(sub, topic_filters: mk_topic_filters({"a/b", 1u8}, {"q0", 0u8}))
          disconnect(sub)
        end
        with_client_io(server) do |pub|
          connect(pub, client_id: "pub")
          # Dropped while offline, so it must not start the deliver loop
          publish(pub, topic: "q0", qos: 0u8)
          pingpong(pub)
          server.vhosts["/"].session("mqtt.sub").message_count.should eq 0
          deliver_loop_started.call(server, "sub").should be_false
          publish(pub, topic: "a/b", payload: "offline".to_slice, qos: 1u8, packet_id: 4u16)
          disconnect(pub)
        end
        deliver_loop_started.call(server, "sub").should be_true
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub")
          String.new(read_publish(sub).payload).should eq "offline"
          sub.should be_silent
          disconnect(sub)
        end
      end
    end

    describe "across a restart" do
      it "leaves nothing for a persistent publish-only client, until it subscribes" do
        with_server(clean_dir: false) do |server|
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            publish(sub, topic: "a/b", qos: 1u8, packet_id: 10u16)
            disconnect(sub)
          end
        end
        with_server do |server|
          Dir.exists?(session_dir.call(server, "sub")).should be_false
          server.vhosts["/"].session?("mqtt.sub").should be_nil
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub").as(MQTT::Protocol::Connack).session_present?.should be_false
            subscribe(sub, topic_filters: mk_topic_filters({"a/b", 1u8}))
            session_files.call(server, "sub").should eq ["msgs.0000000001"]
            with_client_io(server) do |pub|
              connect(pub, client_id: "pub")
              publish(pub, topic: "a/b", payload: "after".to_slice, qos: 1u8, packet_id: 5u16)
              disconnect(pub)
            end
            String.new(read_publish(sub).payload).should eq "after"
            disconnect(sub)
          end
        end
      end

      it "keeps a QoS 2 publisher's session without a store, and its directory goes with it" do
        with_server(clean_dir: false) do |server|
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            publish(pub, topic: "a/b", qos: 2u8, packet_id: 10u16)
            disconnect(pub)
          end
        end
        with_server do |server|
          dir = session_dir.call(server, "pub")
          Dir.children(dir).should eq ["packet_ids.log"]
          server.vhosts["/"].session("mqtt.pub").@msg_store.should be_nil
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub").as(MQTT::Protocol::Connack).session_present?.should be_true
            disconnect(pub)
          end
          server.vhosts["/"].delete_queue("mqtt.pub")
          Dir.exists?(dir).should be_false
        end
      end

      it "delivers the messages a session was restored with" do
        with_server(clean_dir: false) do |server|
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            subscribe(sub, topic_filters: mk_topic_filters({"a/b", 1u8}))
            disconnect(sub)
          end
          with_client_io(server) do |pub|
            connect(pub, client_id: "pub")
            publish(pub, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 6u16)
            publish(pub, topic: "a/b", payload: "2".to_slice, qos: 1u8, packet_id: 7u16)
            disconnect(pub)
          end
        end
        with_server do |server|
          deliver_loop_started.call(server, "sub").should be_true
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            {read_publish(sub), read_publish(sub)}.map { |p| String.new(p.payload) }.should eq({"1", "2"})
            disconnect(sub)
          end
        end
      end

      it "starts no deliver loop for a session restored empty" do
        with_server(clean_dir: false) do |server|
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            subscribe(sub, topic_filters: mk_topic_filters({"a/b", 1u8}))
            with_client_io(server) do |pub|
              connect(pub, client_id: "pub")
              publish(pub, topic: "a/b", payload: "1".to_slice, qos: 1u8, packet_id: 8u16)
              disconnect(pub)
            end
            puback(sub, read_publish(sub).packet_id)
            pingpong(sub) # the PUBACK is handled
            disconnect(sub)
          end
        end
        with_server do |server|
          session_files.call(server, "sub").should eq ["msgs.0000000001"]
          server.vhosts["/"].session("mqtt.sub").message_count.should eq 0
          deliver_loop_started.call(server, "sub").should be_false
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            sub.should be_silent
            with_client_io(server) do |pub|
              connect(pub, client_id: "pub")
              publish(pub, topic: "a/b", payload: "2".to_slice, qos: 1u8, packet_id: 9u16)
              disconnect(pub)
            end
            String.new(read_publish(sub).payload).should eq "2"
            disconnect(sub)
          end
        end
      end
    end
  end
end
