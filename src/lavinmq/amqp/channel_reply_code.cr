module LavinMQ
  module AMQP
    enum ChannelReplyCode : UInt16
      CONTENT_TOO_LARGE   = 311
      NO_CONSUMERS        = 313
      ACCESS_REFUSED      = 403
      NOT_FOUND           = 404
      RESOURCE_LOCKED     = 405
      PRECONDITION_FAILED = 406
      NOT_ALLOWED = 530
      # 540 is marked as connection level reply-code at
      # but also mentioned in text "MUST raise a channel exception with reply code 540 (not implemented)"
      # indicating it's ok to use as channel close reply code as well
      NOT_IMPLEMENTED = 540
      # TODO: Is this reply code ok to use on channel close? Does not look like that from the spec
      UNEXPECTED_FRAME = 505
    end
  end
end
