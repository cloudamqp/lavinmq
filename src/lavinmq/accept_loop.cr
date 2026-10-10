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
    # it was accepted. The next one can be accepted right away.
    CONNECTION_ERRORS = {
      Errno::ECONNABORTED, Errno::EPERM, Errno::EPROTO, Errno::ETIMEDOUT, Errno::ENOPROTOOPT,
      Errno::EOPNOTSUPP, Errno::ENETDOWN, Errno::ENETUNREACH, Errno::EHOSTUNREACH,
    }

    RETRY_DELAY     = 10.milliseconds
    RETRY_DELAY_MAX = 1.second

    # Yields each client accepted on *server*, a `TCPServer` or `UNIXServer`,
    # until it's closed. Other errors than those above are raised.
    def self.each(server, name : String, &)
      delay = RETRY_DELAY
      loop do
        client = begin
          server.accept?
        rescue ex : Socket::Error
          if ex.os_error.in?(RESOURCE_ERRORS)
            Log.error { "#{name} can't accept connections, retrying in #{delay.total_milliseconds.to_i}ms: #{ex.message}" }
            sleep delay
            delay = {delay * 2, RETRY_DELAY_MAX}.min
            next
          elsif ex.os_error.in?(CONNECTION_ERRORS)
            Log.debug { "#{name} failed to accept a connection: #{ex.message}" }
            next
          end
          raise ex
        end
        break unless client
        delay = RETRY_DELAY
        yield client
      end
    end
  end
end
