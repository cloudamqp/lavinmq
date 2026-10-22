require "http/server"

class HTTP::Server
  # The process is out of file descriptors or memory, which passes once
  # connections close
  private ACCEPT_RESOURCE_ERRORS = {Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM}
  private ACCEPT_RETRY_DELAY     = 10.milliseconds
  private ACCEPT_RETRY_DELAY_MAX = 1.second

  @accept_retry_delay = ACCEPT_RETRY_DELAY

  # Called with what accepting a connection raised. The stdlib only logs it
  # and tries again right away. While the process is out of file
  # descriptors that spins without letting other fibers run, so no
  # connection is closed to free a descriptor, and every attempt is logged.
  # This pauses before trying again, longer each time, like Go's HTTP server
  # and `LavinMQ::AcceptLoop`.
  private def handle_exception(e : Exception)
    return previous_def unless e.is_a?(Socket::Error) && e.os_error.in?(ACCEPT_RESOURCE_ERRORS)
    Log.error { "HTTP listener can't accept connections, retrying in #{@accept_retry_delay.total_milliseconds.to_i}ms: #{e.message}" }
    sleep @accept_retry_delay
    @accept_retry_delay = {@accept_retry_delay * 2, ACCEPT_RETRY_DELAY_MAX}.min
  end

  # Called with each accepted connection
  protected def dispatch(io)
    @accept_retry_delay = ACCEPT_RETRY_DELAY
    previous_def
  end
end
