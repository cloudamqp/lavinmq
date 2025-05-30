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
    ClientGetNoAck
    ClientPublish
    ClientPublishConfirm
    ClientRedeliver
    ClientReject
    ConsumerAdded
    ConsumerRemoved
  end
end
