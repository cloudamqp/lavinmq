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

describe "Websocket support" do
  it "should connect over websocket" do
    with_http_server do |http, _|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect
      conn.should_not be_nil
      conn.close
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
          spawn { websocket.run }

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
            spawn { websocket.run }

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
            websocket.close
          end
        end
      end
    end
  end
end
