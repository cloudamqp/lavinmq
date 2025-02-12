module LavinMQ
  module AMQP
    enum ConnectionReplyCode : UInt16
      CONNECTION_FORCED = 320
      INVALID_PATH      = 402
      # 403 is marked as channel level reply-code at but ok to use as connection close reply code as well
      # for example for the authentication_failure_close feature
      ACCESS_REFUSED   = 403
      FRAME_ERROR      = 501
      SYNTAX_ERROR     = 502
      COMMAND_INVALID  = 503
      CHANNEL_ERROR    = 504
      UNEXPECTED_FRAME = 505
      RESOURCE_ERROR   = 506
      NOT_ALLOWED      = 530
      NOT_IMPLEMENTED  = 540
      INTERNAL_ERROR   = 541
    end
  end
end
