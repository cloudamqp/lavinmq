require "socket"
require "mqtt-protocol"

module Booth
  # Minimal MQTT 3.1.1 client: what the IoT scenario needs and no more
  class MqttClient
    class Error < Exception; end

    alias P = MQTT::Protocol

    @io : P::IO

    def initialize(@socket : TCPSocket)
      @io = P::IO.new(@socket)
    end

    def connect(client_id : String, user : String, password : String, clean_session = true, keepalive = 10_u16) : Nil
      P::Connect.new(client_id, clean_session, keepalive, user, password.to_slice, nil).to_io(@io)
      @socket.flush
      ack = P::Packet.from_io(@io)
      unless ack.is_a?(P::Connack) && ack.return_code.accepted?
        raise Error.new("Connection refused: #{ack.inspect}")
      end
    end

    def publish(topic : String, payload : Bytes, qos = 0_u8, packet_id : UInt16? = nil) : Nil
      P::Publish.new(topic, payload, packet_id, false, qos, false).to_io(@io)
    end

    def subscribe(filter : String, qos = 0_u8) : Nil
      P::Subscribe.new([P::Subscribe::TopicFilter.new(filter, qos)], 1_u16).to_io(@io)
      @socket.flush
      ack = P::Packet.from_io(@io)
      raise Error.new("Subscribe failed: #{ack.inspect}") unless ack.is_a?(P::SubAck)
    end

    # Round trip that proves the server (behind any proxy) is still there
    def ping : Nil
      P::PingReq.new.to_io(@io)
      @socket.flush
      until P::Packet.from_io(@io).is_a?(P::PingResp)
      end
    end

    def send_ping : Nil
      P::PingReq.new.to_io(@io)
      @socket.flush
    end

    def read_packet : P::Packet
      P::Packet.from_io(@io)
    end

    def flush : Nil
      @socket.flush
    end

    def close : Nil
      P::Disconnect.new.to_io(@io) rescue nil
      @socket.flush rescue nil
      @socket.close rescue nil
    end

    def closed? : Bool
      @socket.closed?
    end
  end
end
