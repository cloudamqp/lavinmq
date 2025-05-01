require "mqtt-protocol"

module MqttHelpers
  class MqttClient
    def next_packet_id
      @packet_id_generator.next.as(UInt16)
    end

    @packet_id_generator : Iterator(UInt16)

    getter client_id

    def initialize(io : IO)
      @client_id = ""
      @io = MQTT::Protocol::IO.new(io)
      @packet_id_generator = (0u16..).each
    end

    def connect(
      expect_response = true,
      username = "valid_user",
      password = "valid_password",
      client_id = "spec_client",
      keepalive = 30u16,
      will = nil,
      clean_session = true,
      **args,
    )
      connect_args = {
        client_id:     client_id,
        clean_session: clean_session,
        keepalive:     keepalive,
        will:          will,
        username:      username,
        password:      password.to_slice,
      }.merge(args)
      @client_id = connect_args.fetch(:client_id, "").to_s
      MQTT::Protocol::Connect.new(**connect_args).to_io(@io)
      read_packet if expect_response
    end

    def disconnect
      MQTT::Protocol::Disconnect.new.to_io(@io)
      true
    rescue IO::Error
      false
    end

    def subscribe(topic : String, qos : UInt8 = 0u8, expect_response = true)
      filter = MQTT::Protocol::Subscribe::TopicFilter.new(topic, qos)
      MQTT::Protocol::Subscribe.new([filter], packet_id: next_packet_id).to_io(@io)
      read_packet if expect_response
    end

    def unsubscribe(*topics : String, expect_response = true)
      MQTT::Protocol::Unsubscribe.new(topics.to_a, next_packet_id).to_io(@io)
      read_packet if expect_response
    end

    def publish(
      topic : String,
      payload : String,
      qos = 0,
      retain = false,
      packet_id : UInt16? = next_packet_id,
      expect_response = true,
    )
      pub_args = {
        packet_id: packet_id,
        payload:   payload.to_slice,
        topic:     topic,
        dup:       false,
        qos:       qos.to_u8,
        retain:    retain,
      }
      MQTT::Protocol::Publish.new(**pub_args).to_io(@io)
      read_packet if pub_args[:qos].positive? && expect_response
    end

    def puback(packet_id : UInt16?)
      return if packet_id.nil?
      MQTT::Protocol::PubAck.new(packet_id).to_io(@io)
    end

    def puback(packet : MQTT::Protocol::Publish)
      if packet_id = packet.packet_id
        MQTT::Protocol::PubAck.new(packet_id).to_io(@io)
      end
    end

    def ping(expect_response = true)
      MQTT::Protocol::PingReq.new.to_io(@io)
      read_packet if expect_response
    end

    def read_packet
      MQTT::Protocol::Packet.from_io(@io)
    rescue ex : IO::Error
      @io.close
      raise ex
    end

    def close
      @io.close
    end

    def closed?
      @io.closed?
    end
  end
end
