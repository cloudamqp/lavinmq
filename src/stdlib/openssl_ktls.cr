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

  # Returns a string describing the kTLS status: "send+recv", "send", "recv", or nil.
  def ktls_status : String?
    case {ktls_send?, ktls_recv?}
    when {true, true}  then "send+recv"
    when {true, false} then "send"
    when {false, true} then "recv"
    end
  end
end
