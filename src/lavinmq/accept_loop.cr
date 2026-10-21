require "socket"
require "./logger"

module LavinMQ
  # Accepts connections on a listener through errors that pass, instead of
  # letting them stop the listener
  module AcceptLoop
    Log = LavinMQ::Log.for "accept"

    # The process is out of file descriptors or memory. These pass once
    # connections close, but retrying right away would only spin, so the
    # retries are spaced out.
    RESOURCE_ERRORS = {Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM}

    # The connection being accepted failed, for example it was reset before
    # it was accepted. The next one can be accepted right away. On Linux it
    # can also be EHOSTDOWN (112) or ENONET (64), which Errno has no members
    # for there.
    {% begin %}
      CONNECTION_ERRORS = {
        Errno::ECONNABORTED, Errno::EPERM, Errno::EPROTO, Errno::ETIMEDOUT, Errno::ENOPROTOOPT,
        Errno::EOPNOTSUPP, Errno::ENETDOWN, Errno::ENETUNREACH, Errno::EHOSTUNREACH,
        {% if flag?(:linux) %} Errno.new(112), Errno.new(64), {% end %}
      }
    {% end %}

    RETRY_DELAY     = 10.milliseconds
    RETRY_DELAY_MAX = 1.second

    # Yields each client accepted on *server*, a `TCPServer` or `UNIXServer`,
    # until it's closed. After a connection error it accepts again right
    # away. After any other error, like the resource errors above or one
    # that isn't listed, it retries with a growing delay.
    def self.each(server, name : String, &)
      delay = RETRY_DELAY
      loop do
        client = begin
          server.accept?
        rescue ex : Socket::Error
          break if server.closed?
          if ex.os_error.in?(CONNECTION_ERRORS)
            Log.debug { "#{name} failed to accept a connection: #{ex.message}" }
          else
            Log.error { "#{name} can't accept connections, retrying in #{delay.total_milliseconds.to_i}ms: #{ex.message}" }
            sleep delay
            delay = {delay * 2, RETRY_DELAY_MAX}.min
          end
          next
        end
        break unless client
        delay = RETRY_DELAY
        yield client
      end
    end
  end
end
