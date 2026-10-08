require "socket"
require "openssl"

class Socket
  # Shuts the connection down both ways (`shutdown(2)`) without closing the
  # file descriptor, so that fibers blocked reading from or writing to the
  # socket fail right away instead of waiting for a timeout. Errors, e.g.
  # when the peer is already gone or the socket is closed, are ignored.
  def shutdown_read_write : Nil
    begin
      close_write
    rescue IO::Error
    end
    begin
      close_read
    rescue IO::Error
    end
  end
end

abstract class OpenSSL::SSL::Socket < IO
  # Shuts down the underlying socket, see `Socket#shutdown_read_write`.
  # No TLS close_notify is sent.
  def shutdown_read_write : Nil
    io = bio.io
    io.shutdown_read_write if io.responds_to?(:shutdown_read_write)
  end
end
