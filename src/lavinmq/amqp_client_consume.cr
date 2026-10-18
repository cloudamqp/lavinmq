require "amqp-client"

class AMQP::Client::Channel
  # amqp-client registers a consumer only once its ConsumeOk has arrived,
  # and the read loop yields after handing ConsumeOk over so that the
  # registration happens before the next frame is read. With several threads
  # the registering fiber can run on another thread while the read loop goes
  # on, and a delivery that arrives first finds no consumer and is dropped,
  # left unacked on the server. Register the consumer before sending
  # basic.consume instead, under a tag chosen here when none is given (in the
  # format LavinMQ uses for the tags it generates).
  def basic_consume(queue, tag = "", no_ack = true, exclusive = false,
                    block = false, args arguments = Arguments.new, work_pool = 1,
                    &blk : DeliverMessage -> Nil)
    tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}" if tag.empty?
    raise ArgumentError.new("Max allowed work_pool is 1024") if work_pool > 1024
    raise ArgumentError.new("At least one worker required") if work_pool < 1

    deliveries = ::Channel(DeliverMessage).new(@prefetch_count.to_i32)
    @consumers[tag] = deliveries
    begin
      write Frame::Basic::Consume.new(@id, 0_u16, queue, tag, false, no_ack, exclusive, false, arguments)
      expect Frame::Basic::ConsumeOk
    rescue ex
      @consumers.delete(tag)
      raise ex
    end
    done = ::Channel(Exception?).new
    work_pool.times do |i|
      spawn consume(tag, deliveries, done, i, !block, blk),
        name: "AMQPconsumer##{tag} ##{i}"
    end
    if block
      work_pool.times do
        if ex = done.receive
          done.close
          raise ex
        end
      end
      raise_if_closed
    else
      done.close
    end
    tag
  end
end
