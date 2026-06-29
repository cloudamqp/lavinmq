module LavinMQ
  module Shovel
    DEFAULT_ACK_MODE          = AckMode::OnConfirm
    DEFAULT_DELETE_AFTER      = DeleteAfter::Never
    DEFAULT_PREFETCH          = 1000_u16
    DEFAULT_RECONNECT_DELAY   = 5.seconds
    DEFAULT_BATCH_ACK_TIMEOUT = 3.seconds

    enum State
      Starting
      Running
      Stopped
      Paused
      Terminated
      Error
    end

    enum DeleteAfter
      Never
      QueueLength
    end

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    # The per-message disposition a Destination reports for a delivery attempt.
    # The Destination classifies its native result (HTTP status, AMQP confirm)
    # into one of these; the Runner decides what each one does:
    #   Confirmed - delivered; ack the source.
    #   Retry     - transient failure; requeue and retry with backoff.
    #   Reject    - the message is unacceptable; reject without requeue (DLX).
    #   Abort     - the destination is unusable; keep the message and, past a
    #               threshold of consecutive Aborts, error-out the shovel.
    enum Outcome
      Confirmed
      Retry
      Reject
      Abort
    end
  end
end
