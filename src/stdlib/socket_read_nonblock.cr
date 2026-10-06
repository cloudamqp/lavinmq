require "socket"

# Non-blocking reads for sockets, an API the stdlib lacks: Socket#read always
# waits for data, so the buffer to read into has to be allocated up front.
#
# These rely on stdlib internals (Socket's @fd_lock, the event loop's
# wait_readable), so re-verify them, and bump the version here, when
# upgrading Crystal. Replace them if the stdlib gets an equivalent API.
{% unless compare_versions(Crystal::VERSION, "1.21.0") >= 0 && compare_versions(Crystal::VERSION, "1.22.0") < 0 %}
  {% warning "Socket#read_nonblock is only tested with Crystal 1.21, not #{Crystal::VERSION.id}" %}
{% end %}

lib LibC
  {% if flag?(:linux) %}
    MSG_DONTWAIT = 0x40
  {% else %}
    MSG_DONTWAIT = 0x80
  {% end %}
end

class Socket
  # Reads at most `slice.size` bytes into *slice* without waiting for data.
  # Returns the number of bytes read (`0` at end of stream), or `nil` if no
  # data is available. Use `#wait_readable` to wait for data.
  def read_nonblock(slice : Bytes) : Int32?
    check_open
    # like #unbuffered_read, hold the fd's read lock so a concurrent close
    # can't close (and the OS reuse) the fd while we're reading from it
    @fd_lock.read do
      loop do
        ret = LibC.recv(fd, slice, slice.size, LibC::MSG_DONTWAIT)
        return ret.to_i32 if ret >= 0
        case Errno.value
        when Errno::EAGAIN # same as EWOULDBLOCK on Linux and macOS
          return
        when Errno::EINTR
          next
        else
          raise IO::Error.from_errno("read", target: self)
        end
      end
    end
  end

  # Waits until the socket is readable. Raises `IO::TimeoutError` after
  # `read_timeout`, and `IO::Error` if the socket is closed meanwhile.
  def wait_readable : Nil
    check_open
    # a reference keeps a concurrent close from releasing the fd while waiting
    @fd_lock.reference { Crystal::EventLoop.current.wait_readable(self) }
    check_open
  end
end
