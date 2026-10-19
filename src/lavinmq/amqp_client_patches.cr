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

  # amqp-client numbered a publish for its confirm only after writing it and
  # releasing the connection lock. With several threads the read loop can
  # handle the broker's ack or nack for it before then, find nothing to
  # confirm, and drop it, so the publish callback never runs. Concurrent
  # publishers on the channel could also number their publishes out of wire
  # order. Number the publish under the connection lock, before writing it.
  def basic_publish(body : IO | Bytes, bytesize : Int, exchange : String, routing_key = "",
                    mandatory = false, immediate = false, props properties = Properties.new,
                    blk : Proc(Bool, Nil)? = nil) : UInt64
    raise_if_closed

    msgid = 0_u64
    @connection.with_lock(flush: !@tx) do |c|
      if blk && !@confirm_mode
        c.unsafe_write Frame::Confirm::Select.new(@id, true)
        @confirm_mode = true
      end
      if @confirm_mode
        @confirm_lock.synchronize do
          msgid = @confirm_id &+= 1_u64
          @unconfirmed_publishes.push({msgid, blk})
        end
      end
      c.unsafe_write Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, mandatory, immediate)
      c.unsafe_write Frame::Header.new(@id, 60_u16, 0_u16, bytesize.to_u64, properties)
      pos = 0_u32
      body_max = @connection.frame_max - 8
      until pos == bytesize
        length = Math.min(body_max, bytesize.to_u32 - pos)
        case body
        when Bytes
          c.unsafe_write Frame::BytesBody.new(@id, length, body[pos.to_i32, length.to_i32])
        when IO
          c.unsafe_write Frame::Body.new(@id, length, body)
        end
        pos += length
      end
    end
    msgid
  end

  # amqp-client's version enqueued the publishing fiber from the confirm
  # callback and then slept. Now that a confirm can be handled as soon as the
  # publish is written, the callback can run on another thread before that
  # fiber sleeps, and enqueue a fiber that's still running. Wait on a channel.
  def basic_publish_confirm(msg, exchange, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new) : Bool
    confirmed = ::Channel(Bool).new(1)
    basic_publish(msg, exchange, routing_key, mandatory, immediate, properties) do |ok|
      confirmed.send ok
    end
    ok = confirmed.receive
    raise_if_closed
    ok
  end

  def basic_publish_confirm(io : IO, bytesize : Int, exchange : String, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new) : Bool
    confirmed = ::Channel(Bool).new(1)
    basic_publish(io, bytesize, exchange, routing_key, mandatory, immediate, properties) do |ok|
      confirmed.send ok
    end
    ok = confirmed.receive
    raise_if_closed
    ok
  end

  # Runs the confirm callbacks after releasing the confirm lock, which a
  # publish now takes while holding the connection lock: a callback that
  # publishes or acks on the same connection must not hold it.
  private def process_confirm(acked : Bool, delivery_tag : UInt64, multiple : Bool)
    callbacks = [] of Proc(Bool, Nil)
    @confirm_lock.synchronize do
      if @unconfirmed_publishes.first?.try(&.[0]) == delivery_tag
        _, blk = @unconfirmed_publishes.shift
        blk.try { |cb| callbacks << cb }
      elsif multiple
        @unconfirmed_publishes.reject! do |confirm_id, cb|
          break if confirm_id > delivery_tag
          cb.try { |c| callbacks << c }
          true
        end
      elsif idx = @unconfirmed_publishes.bsearch_index { |(confirm_id, _)| confirm_id >= delivery_tag }
        _, blk = @unconfirmed_publishes.delete_at(idx)
        blk.try { |cb| callbacks << cb }
      end
      if @unconfirmed_publishes.empty?
        loop do
          select
          when @unconfirmed_empty.send acked
          else
            break
          end
        end
      end
    end
    callbacks.each &.call(acked)
  end
end
