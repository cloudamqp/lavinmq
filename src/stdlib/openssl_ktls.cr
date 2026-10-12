# Expose kTLS status on OpenSSL::SSL::Socket.
#
# The underlying OpenSSL::SSL::BIO already implements `ktls_send?`/`ktls_recv?`,
# but `bio` is private on the stdlib socket. Reopen the class and surface the
# checks so callers can log whether kTLS is in effect for a given connection.

require "openssl"

abstract class OpenSSL::SSL::Socket < IO
  # Returns `true` if kTLS is active for sending data on this connection.
  def ktls_send? : Bool
    bio.ktls_send?
  end

  # Returns `true` if kTLS is active for receiving data on this connection.
  def ktls_recv? : Bool
    bio.ktls_recv?
  end

  # Returns a string describing the kTLS status: "send+recv", "send", "recv", or "off".
  def ktls_status : String
    case {ktls_send?, ktls_recv?}
    in {true, true}   then "send+recv"
    in {true, false}  then "send"
    in {false, true}  then "recv"
    in {false, false} then "off"
    end
  end
end

{% if OpenSSL.has_constant?(:KTLS) %}
  # The stdlib BIO returns the errno of a failed kTLS recvmsg/sendmsg as the
  # callback's return value, which OpenSSL takes as a successful 0 byte
  # read/write. A read timeout then surfaces as EOF or an SSL error instead of
  # IO::TimeoutError, so idle AMQP clients were disconnected instead of being
  # sent heartbeats. Raise like the non-kTLS path does, from the callback.
  class OpenSSL::BIO
    def self.read_ex(b, buffer, len, readp)
      bio = Box(BIO).unbox(LibCrypto.BIO_get_data(b))
      return previous_def unless bio.ktls_recv?

      size = len > Int32::MAX ? Int32::MAX : len.to_i
      case ret = KTLS.read_record(bio.socket, buffer, size)
      in Int32
        readp.value = LibC::SizeT.new(ret)
        1
      in Errno
        raise_ktls_error(bio.socket, ret, "recvmsg", "Read timed out")
      end
    end

    def self.write_ex(b, data, len, writep)
      bio = Box(BIO).unbox(LibCrypto.BIO_get_data(b))
      return previous_def unless bio.ktls_send_ctrl_msg?

      size = len > Int32::MAX ? Int32::MAX : len.to_i
      case ret = KTLS.send_ctrl_message(bio.socket, bio.ktls_record_type, data, size)
      in Int32
        LibCrypto.BIO_clear_flags(b, LibCrypto::BIO_FLAGS_KTLS_TX_CTRL_MSG)
        writep.value = LibC::SizeT.new(ret)
        1
      in Errno
        raise_ktls_error(bio.socket, ret, "sendmsg", "Write timed out")
      end
    end

    # The polling event loop reports a timeout as ETIMEDOUT, io_uring as ECANCELED
    private def self.raise_ktls_error(socket, errno, syscall, timeout_message) : NoReturn
      raise IO::TimeoutError.new(timeout_message) if errno.in?(Errno::ETIMEDOUT, Errno::ECANCELED)
      raise IO::Error.new("Closed stream") if errno == Errno::EBADF && socket.closed?
      raise IO::Error.from_os_error(syscall, errno, target: socket)
    end
  end
{% end %}
