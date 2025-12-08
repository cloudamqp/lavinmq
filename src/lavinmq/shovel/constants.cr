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

    class FailedDeliveryError < Exception; end
  end
end
