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

# Crystal's `Options::ALL` (0x80000BFF) includes bit 3, which is
# SSL_OP_ENABLE_KTLS in OpenSSL 3. Crystal 1.21 cleared it again after adding
# `ALL`, 1.22 doesn't, so every context had kTLS enabled. Keep kTLS opt-in.
{% if OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
  abstract class OpenSSL::SSL::Context
    protected def initialize(method : LibSSL::SSLMethod)
      previous_def
      remove_options(OpenSSL::SSL::Options::ENABLE_KTLS)
    end
  end
{% end %}

{% if OpenSSL.has_constant?(:KTLS) %}
  # The stdlib BIO passes an Errno to OpenSSL as a successful 0 byte read or
  # write, so a failed read surfaced as EOF and a failed control record (e.g.
  # the close_notify alert) as sent. Raise like the non-kTLS path does.
  # Remove once fixed in Crystal.
  module OpenSSL::KTLS
    def self.read_record(socket : Socket, data : UInt8*, length : Int32) : Int32 | Errno
      raise_on_errno(previous_def, "recvmsg", "Read timed out")
    end

    def self.send_ctrl_message(socket : Socket, record_type : UInt8, data : UInt8*, length : Int32) : Int32 | Errno
      raise_on_errno(previous_def, "sendmsg", "Write timed out")
    end

    private def self.raise_on_errno(ret : Int32 | Errno, syscall : String, timeout_message : String) : Int32
      return ret if ret.is_a?(Int32)
      # ETIMEDOUT from the polling event loop, ECANCELED from io_uring
      raise IO::TimeoutError.new(timeout_message) if ret.in?(Errno::ETIMEDOUT, Errno::ECANCELED)
      raise IO::Error.from_os_error(syscall, ret)
    end
  end
{% end %}
