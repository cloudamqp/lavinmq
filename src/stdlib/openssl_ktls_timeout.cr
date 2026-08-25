# Make kTLS socket timeouts raise `IO::TimeoutError`, like every other socket read.
#
# When kernel TLS is active, `OpenSSL::BIO` doesn't read through the socket's
# normal read path; it calls `Crystal::EventLoop#recvmsg`/`#sendmsg` instead.
# Those two methods are the only socket operations in the event loop that
# *return* a timeout errno rather than raising `IO::TimeoutError`. OpenSSL turns
# that errno into a failed `SSL_read`, so a plain read timeout reaches the
# caller as an `OpenSSL::SSL::Error` (or, via the underlying-EOF branch in
# `OpenSSL::SSL::Socket#unbuffered_read`, as a bare EOF).
#
# That breaks AMQP heartbeats on kTLS connections: the read loop never reaches
# its `IO::TimeoutError` branch, so it never sends a heartbeat frame, never
# applies the `heartbeat + 5` grace period, and closes the connection at the
# first idle `read_timeout` without logging that it was a heartbeat timeout.
#
# The two event loops report a timeout with different errnos: the polling loops
# (epoll/kqueue) use ETIMEDOUT, io_uring cancels the operation and reports
# ECANCELED.

require "openssl"

{% if Crystal::EventLoop.has_constant?(:Polling) %}
  abstract class Crystal::EventLoop::Polling < Crystal::EventLoop
    def recvmsg(socket : ::Socket, message : Pointer(LibC::Msghdr), flags : Int32) : Int32 | Errno
      ret = previous_def
      raise IO::TimeoutError.new("Read timed out") if ret.is_a?(Errno) && ret.etimedout?
      ret
    end

    def sendmsg(socket : ::Socket, message : Pointer(LibC::Msghdr), flags : Int32) : Int32 | Errno
      ret = previous_def
      raise IO::TimeoutError.new("Write timed out") if ret.is_a?(Errno) && ret.etimedout?
      ret
    end
  end
{% end %}

{% if Crystal::EventLoop.has_constant?(:IoUring) %}
  class Crystal::EventLoop::IoUring < Crystal::EventLoop
    def recvmsg(socket : ::Socket, message : Pointer(LibC::Msghdr), flags : Int32) : Int32 | Errno
      ret = previous_def
      raise IO::TimeoutError.new("Read timed out") if ret.is_a?(Errno) && ret.ecanceled?
      ret
    end

    def sendmsg(socket : ::Socket, message : Pointer(LibC::Msghdr), flags : Int32) : Int32 | Errno
      ret = previous_def
      raise IO::TimeoutError.new("Write timed out") if ret.is_a?(Errno) && ret.ecanceled?
      ret
    end
  end
{% end %}
