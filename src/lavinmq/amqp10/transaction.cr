require "../message"

module LavinMQ::AMQP10
  # A local transaction (part 4 of the spec), declared on a coordinator link.
  # Publishes and acknowledgements made in it take effect when it is
  # discharged without failure, as 0-9-1 transactions do on commit.
  class Transaction
    record TxPublish, session : Session, exchange : String, routing_key : String,
      properties : LavinMQ::AMQP::Properties, bodysize : UInt64, offset : Int64
    record TxAck, session : Session, first : UInt32, last : UInt32, outcome : Outcome,
      annotations : Bytes?, settled : Bool

    getter id : UInt64
    getter coordinator : CoordinatorLink
    @publishes = Array(TxPublish).new
    @acks = Array(TxAck).new

    # Bodies are written to the connection's transaction body file, shared by
    # its open transactions.
    def initialize(@id : UInt64, @coordinator : CoordinatorLink, @body_file : File)
    end

    def txn_id : Bytes
      Transaction.txn_id(@id)
    end

    def self.txn_id(id : UInt64) : Bytes
      bytes = Bytes.new(8)
      IO::ByteFormat::NetworkEndian.encode(id, bytes)
      bytes
    end

    def add_publish(session : Session, target : PublishAddress, properties : LavinMQ::AMQP::Properties, body : Bytes) : Nil
      offset = @body_file.seek(0, IO::Seek::End).pos
      @body_file.write(body)
      @publishes << TxPublish.new(session, target.exchange, target.routing_key, properties, body.bytesize.to_u64, offset)
    end

    # `annotations` are a modified outcome's message-annotations, a view into
    # the frame buffer, so they are copied.
    def add_ack(session : Session, first : UInt32, last : UInt32, outcome : Outcome,
                annotations : Bytes?, settled : Bool) : Nil
      @acks << TxAck.new(session, first, last, outcome, annotations.try(&.dup), settled)
    end

    # Acknowledgements are applied first, as in 0-9-1. Unroutable messages
    # are dropped, like 0-9-1 publishes that are not mandatory.
    def commit(vhost : VHost) : Nil
      @acks.each do |ack|
        ack.session.settle_deliveries(ack.first, ack.last, ack.outcome, ack.annotations, ack.settled)
      end
      @acks.clear
      @publishes.each do |publish|
        @body_file.pos = publish.offset
        msg = Message.new(RoughTime.unix_ms, publish.exchange, publish.routing_key,
          publish.properties, publish.bodysize, @body_file)
        publish.session.publish(msg)
      end
      @publishes.clear
      vhost.sync
    end

    # Deliveries whose acknowledgement the client settled are forgotten by it,
    # so they are requeued; unsettled ones stay unacknowledged on their links.
    def rollback : Nil
      @acks.each do |ack|
        next unless ack.settled
        ack.session.settle_deliveries(ack.first, ack.last, Outcome::Released, nil, true)
      end
      @acks.clear
      @publishes.clear
    end
  end
end
