require "http/server"
require "../accept_loop"

module LavinMQ
  module HTTP
    # The stdlib's HTTP server tries to accept again right away when
    # accepting fails. While the process is out of file descriptors that
    # spins without letting other fibers run, so no connection is closed to
    # free a descriptor, and every attempt is logged. This server pauses
    # before trying again, like `AcceptLoop`.
    class RetryingServer < ::HTTP::Server
      @accept_retry_delay = AcceptLoop::RETRY_DELAY

      # Called by the stdlib with what accepting a connection raised
      private def handle_exception(e : Exception)
        return super unless e.is_a?(Socket::Error) && e.os_error.in?(AcceptLoop::RESOURCE_ERRORS)
        AcceptLoop::Log.error { "HTTP listener can't accept connections, retrying in #{@accept_retry_delay.total_milliseconds.to_i}ms: #{e.message}" }
        sleep @accept_retry_delay
        @accept_retry_delay = {@accept_retry_delay * 2, AcceptLoop::RETRY_DELAY_MAX}.min
      end

      # Called by the stdlib with each accepted connection
      protected def dispatch(io)
        @accept_retry_delay = AcceptLoop::RETRY_DELAY
        super
      end
    end
  end
end
