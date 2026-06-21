module LavinMQ
  module AMQP10
    # Base error for the AMQP 1.0 implementation.
    class Error < Exception
      # Raised while decoding malformed wire data.
      class Decode < Error; end

      # Raised when the peer violates the protocol (maps to an AMQP error condition).
      class NotImplemented < Error; end
    end

    # AMQP 1.0 symbolic error conditions (amqp-core-transport-v1.0 §2.8.15-2.8.16).
    module ErrorCondition
      INTERNAL_ERROR      = "amqp:internal-error"
      NOT_FOUND           = "amqp:not-found"
      UNAUTHORIZED_ACCESS = "amqp:unauthorized-access"
      DECODE_ERROR        = "amqp:decode-error"
      RESOURCE_LIMIT      = "amqp:resource-limit-exceeded"
      NOT_ALLOWED         = "amqp:not-allowed"
      INVALID_FIELD       = "amqp:invalid-field"
      NOT_IMPLEMENTED     = "amqp:not-implemented"
      PRECONDITION_FAILED = "amqp:precondition-failed"
      ILLEGAL_STATE       = "amqp:illegal-state"
      # connection errors
      CONNECTION_FORCED = "amqp:connection:forced"
      FRAMING_ERROR     = "amqp:connection:framing-error"
      # link errors
      LINK_DETACH_FORCED  = "amqp:link:detach-forced"
      LINK_TRANSFER_REFUS = "amqp:link:transfer-limit-exceeded"
    end
  end
end
