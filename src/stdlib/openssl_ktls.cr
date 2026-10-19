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
  # The stdlib BIO passes an Errno to OpenSSL as a 0 byte read, so a timeout
  # surfaced as EOF. Remove once fixed in Crystal.
  module OpenSSL::KTLS
    def self.read_record(socket : Socket, data : UInt8*, length : Int32) : Int32 | Errno
      ret = previous_def
      # ETIMEDOUT from the polling event loop, ECANCELED from io_uring
      if ret.in?(Errno::ETIMEDOUT, Errno::ECANCELED)
        raise IO::TimeoutError.new("Read timed out")
      end
      ret
    end
  end
{% end %}
