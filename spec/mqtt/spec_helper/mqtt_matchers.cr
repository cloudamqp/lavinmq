module MqttMatchers
  struct ClosedExpectation
    include MqttHelpers

    def match(actual : MQTT::Protocol::IO)
      return true if actual.closed?
      read_packet(actual)
      false
    rescue e : IO::Error
      true
    end

    def failure_message(actual_value)
      "Expected #{actual_value.pretty_inspect} to be closed"
    end

    def negative_failure_message(actual_value)
      "Expected #{actual_value.pretty_inspect} to be open"
    end
  end

  def be_closed
    ClosedExpectation.new
  end
end
