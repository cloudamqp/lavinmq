module LavinMQ
  enum EventType
    ChannelCreated
    ChannelClosed
    ConnectionCreated
    ConnectionClosed
    QueueDeclared
    QueueDeleted
    ClientAck
    ClientDeliver
    ClientGet
    ClientPublish
    ClientPublishConfirm
    ClientRedeliver
    ClientReject
  end
end
