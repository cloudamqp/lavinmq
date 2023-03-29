module LavinMQ
  module HTTP
    {% if flag?(:linux) %}
      INTERNAL_UNIX_SOCKET = "@/tmp/lavinmqctl.sock"
    {% else %}
      INTERNAL_UNIX_SOCKET = "/tmp/lavinmqctl.sock"
    {% end %}
  end
end
