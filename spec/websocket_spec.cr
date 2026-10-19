require "http/web_socket"
require "./spec_helper"

private def connect_amqp(websocket : HTTP::WebSocket)
  AMQP::Client::Connection.start(
    AMQP::Client::WebSocketIO.new(websocket),
    "guest",
    "guest",
    "/",
    10u16,   # channel max
    4096u32, # frame max
    0u16,    # heartbeat
    AMQP::Client::ConnectionInformation.new  )
end

private def connect_mqtt(http, will : MQTT::Protocol::Will? = nil, &)
  ws = HTTP::WebSocket.new(http.addr.address, path: "/mqtt", port: http.addr.port)
  connect = MQTT::Protocol::Connect.new(client_id: "ws-close", clean_session: true, keepalive: 30u16,
    username: "guest", password: "guest".to_slice, will: will)
  connack = Channel(Nil).new(1)
  closed = Channel(HTTP::WebSocket::CloseCode).new(1)
  ws.on_binary { |_| connack.send nil unless connack.closed? }
  ws.on_close { |code, _| closed.send code }
  spawn do
    ws.run
  rescue IO::Error
  end
  ws.stream { |io| connect.to_io(MQTT::Protocol::IO.new(io)) }
  connack.receive
  connack.close
  yield ws
  select
  when code = closed.receive then code
  when timeout(3.seconds) then fail "WebSocket not closed"
  end
end

private def with_websocket_io(&)
  peer, socket = UNIXSocket.pair
  io = LavinMQ::WebSocketIO.new(socket)
  client = HTTP::WebSocket::Protocol.new(peer, masked: true)
  yield io, client, peer, socket
ensure
  peer.try &.close
  socket.try &.close
end

describe "Websocket support" do
  describe "sends a close frame" do
    it "when an MQTT client disconnects" do
      with_http_server do |http, _|
        code = connect_mqtt(http) do |ws|
          ws.stream { |io| MQTT::Protocol::Disconnect.new.to_io(MQTT::Protocol::IO.new(io)) }
        end
        code.should_not eq HTTP::WebSocket::CloseCode::AbnormalClosure
      end
    end

    it "when an MQTT client closes the WebSocket" do
      with_http_server do |http, _|
        code = connect_mqtt(http) do |ws|
          # HTTP::WebSocket#close would close the client's socket before the reply arrives
          ws.@ws.send(Bytes[0x03, 0xe8], HTTP::WebSocket::Protocol::Opcode::CLOSE)
        end
        code.should_not eq HTTP::WebSocket::CloseCode::AbnormalClosure
      end
    end

    it "after handling a DISCONNECT received together with the close frame" do
      with_http_server do |http, s|
        will = MQTT::Protocol::Will.new(topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
        code = connect_mqtt(http, will) do |ws|
          frames = IO::Memory.new
          protocol = HTTP::WebSocket::Protocol.new(frames, masked: true)
          disconnect = IO::Memory.new
          MQTT::Protocol::Disconnect.new.to_io(MQTT::Protocol::IO.new(disconnect))
          protocol.send(disconnect.to_slice)
          protocol.send(Bytes[0x03, 0xe8], HTTP::WebSocket::Protocol::Opcode::CLOSE)
          socket = ws.@ws.@io
          socket.write(frames.to_slice)
          socket.flush
        end
        code.should_not eq HTTP::WebSocket::CloseCode::AbnormalClosure
        wait_for { s.vhosts["/"].connections.none?(LavinMQ::MQTT::Client) }
        s.vhosts["/"].exchange("mqtt.default").publish_in_count.should eq 0
      end
    end
  end

  it "should connect over websocket" do
    with_http_server do |http, _|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect
      conn.should_not be_nil
      conn.close
    end
  end

  it "sets tcp_send_timeout as write timeout on wss connections" do
    LavinMQ::Config.instance.tcp_send_timeout = 7
    with_amqp_server do |s|
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"
      addr = s.http_server.bind_tls("127.0.0.1", 0, ctx)
      spawn(name: "https listen") { s.http_server.listen }
      Fiber.yield
      conn = AMQP::Client.new("wss://#{addr}?verify=none").connect
      wait_for { s.vhosts["/"].connections.any?(LavinMQ::AMQP::Client) }
      client = s.vhosts["/"].connections.first.as(LavinMQ::AMQP::Client)
      ws_io = client.@socket.as(LavinMQ::WebSocketIO)
      tls = ws_io.@socket.as(OpenSSL::SSL::Socket)
      tls.write_timeout.should eq 7.seconds
      conn.close
    end
  end

  it "shuts down the underlying connection of wss connections" do
    with_amqp_server do |s|
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"
      addr = s.http_server.bind_tls("127.0.0.1", 0, ctx)
      spawn(name: "https listen") { s.http_server.listen }
      Fiber.yield
      conn = AMQP::Client.new("wss://#{addr}?verify=none").connect
      wait_for { s.vhosts["/"].connections.any?(LavinMQ::AMQP::Client) }
      client = s.vhosts["/"].connections.first.as(LavinMQ::AMQP::Client)
      client.@socket.as(LavinMQ::WebSocketIO).shutdown_read_write
      wait_for { client.closed? }
      wait_for { conn.closed? }
    end
  end

  describe LavinMQ::WebSocketIO do
    it "answers a ping with a pong" do
      with_websocket_io do |io, client|
        client.ping("hello")
        read = Channel(Int32).new
        spawn { read.send io.read(Bytes.new(16)) }
        buffer = Bytes.new(16)
        info = client.receive(buffer)
        info.opcode.should eq HTTP::WebSocket::Protocol::Opcode::PONG
        String.new(buffer[0, info.size]).should eq "hello"
        client.send(Bytes.empty, :close)
        read.receive.should eq 0
      end
    end

    it "reads a fragmented message as a stream" do
      with_websocket_io do |io, client|
        client.send("ab".to_slice, :binary, flags: :none)
        client.send("cd".to_slice, :continuation)
        buffer = Bytes.new(4)
        io.read_fully(buffer)
        String.new(buffer).should eq "abcd"
      end
    end

    it "closes the connection on a text frame" do
      with_websocket_io do |io, client|
        client.send("text")
        io.read(Bytes.new(16)).should eq 0
        buffer = Bytes.new(16)
        info = client.receive(buffer)
        info.opcode.should eq HTTP::WebSocket::Protocol::Opcode::CLOSE
        IO::ByteFormat::NetworkEndian.decode(UInt16, buffer).should eq 1003
      end
    end

    it "replies to a close frame after the frame being written, with the same code" do
      with_websocket_io do |io, client|
        frame = Bytes.new(1000, 1u8)
        sent = 0
        # The peer doesn't read yet, so the socket buffer fills and a frame stays in flight
        spawn do
          loop do
            io.write(frame)
            io.flush
            sent += 1
          end
        rescue IO::Error
        end
        sleep 50.milliseconds
        client.send(Bytes[0x03, 0xe8], :close)
        read = Channel(Int32).new
        spawn { read.send io.read(Bytes.new(1)) }
        buffer = Bytes.new(64 * 1024)
        frames = 0
        loop do
          info = client.receive(buffer)
          break if info.opcode.close?
          info.opcode.should eq HTTP::WebSocket::Protocol::Opcode::BINARY
          info.size.should eq frame.size
          buffer[0, info.size].should eq frame
          frames += 1
        end
        IO::ByteFormat::NetworkEndian.decode(UInt16, buffer).should eq 1000
        read.receive.should eq 0
        frames.should be >= sent
        expect_raises(IO::Error, "Closed stream") { io.write(frame); io.flush }
      end
    end

    it "doesn't raise when the connection is gone before the close frame is sent" do
      with_websocket_io do |io, _, _, socket|
        socket.shutdown_read_write
        io.close
        io.shutdown_read_write
      end
    end
  end

  it "tracks AMQP websocket connections on the vhost" do
    with_http_server do |http, s|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect

      wait_for { s.vhosts["/"].connections.any?(LavinMQ::AMQP::Client) }

      conn.close
      wait_for { s.vhosts["/"].connections.none?(LavinMQ::AMQP::Client) }
    end
  end

  it "can publish large messages" do
    with_http_server do |http, _|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect
      ch = conn.channel
      q = ch.queue
      q.publish "b" * 150_000
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.body_io.to_s.should eq "b" * 150_000 if msg
      conn.close
    end
  end

  describe "when request 'Sec-WebSocket-Protocol'" do
    describe "is set to 'stomp,amqp,mqtt'" do
      it "should set response 'Sec-WebSocket-Protocol' to 'amqp' (first match)" do
        with_http_server do |http, _|
          headers = ::HTTP::Headers{
            "Upgrade"                => "websocket",
            "Connection"             => "Upgrade",
            "Sec-WebSocket-Version"  => "13",
            "Sec-WebSocket-Protocol" => "stomp, amqp, mqtt",
            "Sec-WebSocket-Key"      => "random",
          }
          response = http.get("/", headers)
          response.headers["Sec-WebSocket-Protocol"]?.should eq "amqp"
        end
      end
    end

    describe "is set to 'invalid'" do
      header = "invalid"

      it "should not set response 'Sec-WebSocket-Protocol'" do
        with_http_server do |http, _|
          headers = ::HTTP::Headers{
            "Upgrade"                => "websocket",
            "Connection"             => "Upgrade",
            "Sec-WebSocket-Version"  => "13",
            "Sec-WebSocket-Protocol" => header,
            "Sec-WebSocket-Key"      => "random",
          }
          response = http.get("/", headers)
          response.headers.has_key?("Sec-WebSocket-Protocol").should be_false
        end
      end

      it "should accept amqp client" do
        with_http_server do |http, _|
          headers = ::HTTP::Headers{"Sec-WebSocket-Protocol" => header}
          websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)
          connect_amqp(websocket)
          websocket.close
        end
      end

      it "should not accept mqtt client" do
        with_http_server do |http, s|
          s.@config.default_user_only_loopback = false
          headers = ::HTTP::Headers{
            "Sec-WebSocket-Protocol" => header,
          }
          websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)

          connect = MQTT::Protocol::Connect.new(
            client_id: "client_id",
            clean_session: false,
            keepalive: 30u16,
            username: "guest",
            password: "guest".to_slice,
            will: nil,
          )

          ch = Channel(Nil).new
          websocket.on_binary do |bytes|
            pkt = MQTT::Protocol::Packet.from_io(IO::Memory.new(bytes))
            fail("received unexpected #{pkt}")
          rescue
            ch.close # close to signal "failure"
          end
          websocket.on_close do
            ch.close
          end
          spawn do
            websocket.run
          rescue IO::Error
            # The server closes the connection right after its close frame
          end

          websocket.stream { |io| connect.to_io(MQTT::Protocol::IO.new(io)) }

          expect_raises(Channel::ClosedError) do
            select
            when ch.receive # this is closed = error
              fail("received data?")
            when timeout(1.second)
              fail("Socket not closed?")
            end
          end
        end
      end
    end

    {"amqp", "amqpish"}.each do |header|
      describe "is set to '#{header}'" do
        it "should set response 'Sec-WebSocket-Protocol' to '#{header}'" do
          with_http_server do |http, _|
            headers = ::HTTP::Headers{
              "Upgrade"                => "websocket",
              "Connection"             => "Upgrade",
              "Sec-WebSocket-Version"  => "13",
              "Sec-WebSocket-Protocol" => header,
              "Sec-WebSocket-Key"      => "random",
            }
            response = http.get("/", headers)
            response.headers["Sec-WebSocket-Protocol"]?.should eq header
          end
        end

        it "should accept amqp client" do
          with_http_server do |http, _|
            headers = ::HTTP::Headers{"Sec-WebSocket-Protocol" => header}
            websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)
            connect_amqp(websocket)
            websocket.close
          end
        end

        it "should not accept mqtt client" do
          with_http_server do |http, s|
            s.@config.default_user_only_loopback = false
            headers = ::HTTP::Headers{
              "Sec-WebSocket-Protocol" => header,
            }
            websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)

            connect = MQTT::Protocol::Connect.new(
              client_id: "client_id",
              clean_session: false,
              keepalive: 30u16,
              username: "guest",
              password: "guest".to_slice,
              will: nil,
            )

            ch = Channel(Nil).new
            websocket.on_binary do |bytes|
              pkt = MQTT::Protocol::Packet.from_io(IO::Memory.new(bytes))
              fail("received unexpected #{pkt}")
            rescue
              ch.close # close to signal "failure"
            end
            websocket.on_close do
              ch.close
            end
            spawn do
              websocket.run
            rescue IO::Error
              # The server closes the connection right after its close frame
            end

            websocket.stream { |io| connect.to_io(MQTT::Protocol::IO.new(io)) }

            expect_raises(Channel::ClosedError) do
              select
              when ch.receive # this is closed = error
                fail("received data?")
              when timeout(1.second)
                fail("Socket not closed?")
              end
            end
          end
        end
      end
    end

    {"mqtt", "mqttish"}.each do |header|
      describe "is set to '#{header}'" do
        it "should set response 'Sec-WebSocket-Protocol' to '#{header}'" do
          with_http_server do |http, _|
            headers = ::HTTP::Headers{
              "Upgrade"                => "websocket",
              "Connection"             => "Upgrade",
              "Sec-WebSocket-Version"  => "13",
              "Sec-WebSocket-Protocol" => header,
              "Sec-WebSocket-Key"      => "random",
            }
            response = http.get("/", headers)
            response.headers["Sec-WebSocket-Protocol"]?.should eq header
          end
        end

        it "should accept mqtt client" do
          with_http_server do |http, _|
            headers = ::HTTP::Headers{
              "Sec-WebSocket-Protocol" => header,
            }
            websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)

            connect = MQTT::Protocol::Connect.new(
              client_id: "client_id",
              clean_session: false,
              keepalive: 30u16,
              username: "guest",
              password: "guest".to_slice,
              will: nil,
            )

            ch = Channel(MQTT::Protocol::Packet).new
            websocket.on_binary do |bytes|
              ch.send MQTT::Protocol::Packet.from_io(IO::Memory.new(bytes))
              websocket.close
            end
            spawn { websocket.run }

            websocket.stream { |io| connect.to_io(MQTT::Protocol::IO.new(io)) }

            select
            when pkt = ch.receive
              pkt.should be_a MQTT::Protocol::Connack
              pkt.as(MQTT::Protocol::Connack).return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            when timeout(1.second)
              websocket.close
              fail("no response?")
            end
          end
        end

        it "should not accept amqp client" do
          with_http_server do |http, _|
            headers = ::HTTP::Headers{
              "Sec-WebSocket-Protocol" => header,
            }
            websocket = ::HTTP::WebSocket.new(http.addr.address, path: "", port: http.addr.port, headers: headers)
            expect_raises(IO::Error) do
              connect_amqp(websocket)
            end
            begin
              websocket.close
            rescue IO::Error
              # The server closes the connection right after its close frame
            end
          end
        end
      end
    end
  end
end
