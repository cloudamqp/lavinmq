module LavinMQ
  module MQTT
    # Controls how the MQTT client_id is validated against the
    # authenticated username at connect.
    enum ClientIdValidation
      None           # any client_id accepted
      Username       # client_id must equal the username
      UsernamePrefix # client_id must start with the username
    end
  end
end
