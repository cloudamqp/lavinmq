module LavinMQ
  module HTTP
    {% if flag?(:linux) %}
      INTERNAL_UNIX_SOCKET = "/dev/shm/lavinmq-http.sock"
    {% else %}
      INTERNAL_UNIX_SOCKET = "/tmp/lavinmq-http.sock"
    {% end %}
  end
end
