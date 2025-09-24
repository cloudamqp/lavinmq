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
    ClientDeliverNoAck
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
