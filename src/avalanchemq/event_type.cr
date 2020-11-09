module AvalancheMQ
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
    ClientRedeliver
    ClientReject
  end
end
