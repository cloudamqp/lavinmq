module LavinMQ
  module Federation
    DEFAULT_PREFETCH        = 1000_u16
    DEFAULT_RECONNECT_DELAY = 1.second
    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFAULT_MAX_HOPS        = 1_i64
    DEFAULT_EXPIRES         = nil
    DEFAULT_MSG_TTL         = nil

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end
  end
end
