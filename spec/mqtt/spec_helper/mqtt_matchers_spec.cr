require "./mqtt_helpers_spec"

module MqttMatchers
  struct ClosedExpectation
    include MqttHelpers

    def match(actual : MQTT::Protocol::IO)
      return true if actual.closed?
      MQTT::Protocol::Packet.from_io(actual)
      false
    rescue IO::TimeoutError
      false
    rescue IO::Error
      true
    end

    def failure_message(actual_value)
      "Expected socket to be closed"
    end

    def negative_failure_message(actual_value)
      "Expected socket to be open"
    end
  end

  def be_closed
    ClosedExpectation.new
  end

  struct EmptyMatcher
    include MqttHelpers

    def match(actual)
      ping(actual)
      resp = read_packet(actual)
      resp.is_a?(MQTT::Protocol::PingResp)
    end

    def failure_message(actual_value)
      "Expected socket to be drained"
    end

    def negative_failure_message(actual_value)
      "Expected socket to not be drained"
    end
  end

  def be_drained
    EmptyMatcher.new
  end

  class SilentExpectation
    @packet : MQTT::Protocol::Packet?

    def match(actual : MQTT::Protocol::IO)
      socket = actual.io.as(Socket)
      timeout = socket.read_timeout
      socket.read_timeout = MqttHelpers::SILENCE_TIMEOUT
      begin
        @packet = MQTT::Protocol::Packet.from_io(actual)
        false
      rescue IO::TimeoutError
        true
      ensure
        socket.read_timeout = timeout
      end
    end

    def failure_message(actual_value)
      "Expected no packet within #{MqttHelpers::SILENCE_TIMEOUT}, got #{@packet.inspect}"
    end

    def negative_failure_message(actual_value)
      "Expected a packet within #{MqttHelpers::SILENCE_TIMEOUT}"
    end
  end

  def be_silent
    SilentExpectation.new
  end
end
