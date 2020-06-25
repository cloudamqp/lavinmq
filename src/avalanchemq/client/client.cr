require "logger"
require "openssl"
require "socket"
require "../message"
require "./channel"
require "../user"
require "../stats"
require "../sortable_json"
require "../rough_time"

module AvalancheMQ
  abstract class Client
    include Stats
    include SortableJSON

    abstract def send(frame : AMQP::Frame)
    abstract def to_json(json : JSON::Builder)
    abstract def connection_details
    abstract def deliver(frame : AMQP::Frame, msg : Message)
    abstract def channel_name_prefix

    setter direct_reply_consumer_tag
    getter vhost, channels, log, exclusive_queues,
      name, direct_reply_consumer_tag, client_properties, user

    @client_properties : AMQP::Table
    @connected_at : Int64
    @direct_reply_consumer_tag : String?
    @log : Logger
    @running = true
    @last_heartbeat = RoughTime.utc
    rate_stats(%w(send_oct recv_oct))

    def initialize(@name : String, @vhost : VHost, @user : User,
                   @log : Logger,
                   @client_properties = AMQP::Table.new)
      @connected_at = Time.utc.to_unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @exclusive_queues = Array(Queue).new
      @vhost.add_connection(self)
      @log.debug "Connected"
    end

    def state
      !@running ? "closed" : (@vhost.flow? ? "running" : "flow")
    end

    private def with_channel(frame)
      if ch = @channels[frame.channel]?
        if ch.running?
          yield ch
        else
          case frame
          when AMQP::Frame::Basic::Publish, AMQP::Frame::Header
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          when AMQP::Frame::Body
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            frame.body.skip(frame.body_size)
          else
            yield ch
          end
        end
      else
        @log.error { "Channel #{frame.channel} not open" }
        close_connection(frame, 504_u16, "CHANNEL_ERROR - Channel #{frame.channel} not open")
      end
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
      send AMQP::Frame::Channel::OpenOk.new(frame.channel)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def process_frame(frame)
      @recv_oct_count += 8_u64 + frame.bytesize
      case frame
      when AMQP::Frame::Connection::Close
        send AMQP::Frame::Connection::CloseOk.new
        return false
      when AMQP::Frame::Connection::CloseOk
        @log.debug "Disconnected"
        cleanup
        return false
      when AMQP::Frame::Channel::Open
        open_channel(frame)
      when AMQP::Frame::Channel::Close
        @channels.delete(frame.channel).try &.close
        send AMQP::Frame::Channel::CloseOk.new(frame.channel)
      when AMQP::Frame::Channel::CloseOk
        @channels.delete(frame.channel).try &.close
      when AMQP::Frame::Channel::Flow
        @channels[frame.channel].client_flow(frame.active)
      when AMQP::Frame::Channel::FlowOk
        # noop
      when AMQP::Frame::Confirm::Select
        with_channel frame, &.confirm_select(frame)
      when AMQP::Frame::Exchange::Declare
        declare_exchange(frame)
      when AMQP::Frame::Exchange::Delete
        delete_exchange(frame)
      when AMQP::Frame::Exchange::Bind
        bind_exchange(frame)
      when AMQP::Frame::Exchange::Unbind
        unbind_exchange(frame)
      when AMQP::Frame::Queue::Declare
        declare_queue(frame)
      when AMQP::Frame::Queue::Bind
        bind_queue(frame)
      when AMQP::Frame::Queue::Unbind
        unbind_queue(frame)
      when AMQP::Frame::Queue::Delete
        delete_queue(frame)
      when AMQP::Frame::Queue::Purge
        purge_queue(frame)
      when AMQP::Frame::Basic::Publish
        start_publish(frame)
      when AMQP::Frame::Header
        with_channel frame, &.next_msg_headers(frame)
      when AMQP::Frame::Body
        with_channel frame, &.add_content(frame)
      when AMQP::Frame::Basic::Consume
        consume(frame)
      when AMQP::Frame::Basic::Get
        basic_get(frame)
      when AMQP::Frame::Basic::Ack
        with_channel frame, &.basic_ack(frame)
      when AMQP::Frame::Basic::Reject
        with_channel frame, &.basic_reject(frame)
      when AMQP::Frame::Basic::Nack
        with_channel frame, &.basic_nack(frame)
      when AMQP::Frame::Basic::Cancel
        with_channel frame, &.cancel_consumer(frame)
      when AMQP::Frame::Basic::Qos
        with_channel frame, &.basic_qos(frame)
      when AMQP::Frame::Basic::Recover
        with_channel frame, &.basic_recover(frame)
      when AMQP::Frame::Heartbeat
        @last_heartbeat = RoughTime.utc
        send frame
      else
        raise AMQP::Error::NotImplemented.new(frame)
      end
      true
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      if ex.channel.zero?
        close_connection(ex, 540_u16, "NOT_IMPLEMENTED")
      else
        close_channel(ex, 540_u16, "NOT_IMPLEMENTED")
      end
      true
    rescue ex : Exception
      raise ex unless frame.is_a? AMQP::Frame::Method
      @log.error { "#{ex.inspect}, when processing frame" }
      @log.debug { ex.inspect_with_backtrace }
      close_channel(frame, 541_u16, "INTERNAL_ERROR")
      true
    end

    protected def cleanup
      @running = false
      @log.debug "Cleaning up"
      @exclusive_queues.each(&.close)
      @exclusive_queues.clear
      @channels.each_value &.close
      @channels.clear
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    def close(reason = nil)
      reason ||= "Connection closed"
      @log.info { "Closing, #{reason}" }
      close_frame = AMQP::Frame::Connection::Close.new(320_u16, reason.to_s, 0_u16, 0_u16)
      send(close_frame) || cleanup
      @running = false
    end

    def closed?
      !@running
    end

    def close_channel(frame, code, text)
      case frame
      when AMQP::Frame::Header, AMQP::Frame::Body
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, 0, 0)
      else
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, frame.class_id, frame.method_id)
      end
      @channels[frame.channel].running = false
    end

    def close_connection(frame, code, text)
      @log.info { "Closing, #{text}" }
      case frame
      when AMQ::Protocol::Frame::Method
        send AMQP::Frame::Connection::Close.new(code, text, frame.class_id, frame.method_id)
      else
        send AMQP::Frame::Connection::Close.new(code, text, 0_u16, 0_u16)
      end
      @running = false
    end

    def direct_reply_channel
      if direct_reply_consumer_tag
        @vhost.direct_reply_channels[direct_reply_consumer_tag]?
      end
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def send_access_refused(frame, text)
      @log.warn { "Access refused channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 403_u16, "ACCESS_REFUSED - #{text}")
    end

    def send_not_found(frame, text = "")
      @log.warn { "Not found channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 404_u16, "NOT_FOUND - #{text}")
    end

    def send_resource_locked(frame, text)
      @log.warn { "Resource locked channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 405_u16, "RESOURCE_LOCKED - #{text}")
    end

    def send_precondition_failed(frame, text)
      @log.warn { "Precondition failed channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 406_u16, "PRECONDITION_FAILED - #{text}")
    end

    private def declare_exchange(frame)
      name = frame.exchange_name
      if e = @vhost.exchanges.fetch(name, nil)
        if frame.passive || e.match?(frame)
          unless frame.no_wait
            send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_precondition_failed(frame, "Existing exchange '#{name}' declared with other arguments")
        end
      elsif frame.passive
        send_not_found(frame, "Exchange '#{name}' doesn't exists")
      elsif name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        ae = frame.arguments["x-alternate-exchange"]?.try &.as?(String)
        ae_ok = ae.nil? || (@user.can_write?(@vhost.name, ae) && @user.can_read?(@vhost.name, name))
        unless @user.can_config?(@vhost.name, name) && ae_ok
          send_access_refused(frame, "User doesn't have permissions to declare exchange '#{name}'")
          return
        end
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeclareOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.exchange_name.starts_with? "amq."
          send_access_refused(frame, "Not allowed to use the amq. prefix")
          return
        elsif !@user.can_config?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
        elsif e.internal
          send_access_refused(frame, "Not allowed to delete internal exchange")
        else
          @vhost.apply(frame)
          send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
        end
      else
        send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_queue(frame)
      q = @vhost.queues.fetch(frame.queue_name, nil)
      unless q
       send AMQP::Frame::Queue::DeleteOk.new(frame.channel, 0_u32) unless frame.no_wait
       return
      end
      if q.exclusive && !exclusive_queues.includes? q
        send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
      elsif frame.if_unused && !q.consumer_count.zero?
        send_precondition_failed(frame, "Queue '#{q.name}' in use")
      elsif frame.if_empty && !q.message_count.zero?
        send_precondition_failed(frame, "Queue '#{q.name}' is not empty")
      elsif !@user.can_config?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have permissions to delete queue '#{q.name}'")
      elsif q.internal?
        send_access_refused(frame, "Not allowed to delete internal queue")
      else
        size = q.message_count
        @vhost.apply(frame)
        @exclusive_queues.delete(q) if q.exclusive
        send AMQP::Frame::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        redeclare_queue(frame, q)
      elsif frame.passive
        send_not_found(frame, "Queue '#{frame.queue_name}' doesn't exists")
      elsif frame.queue_name =~ /^amq\.(rabbitmq|direct)\.reply-to/
        unless frame.no_wait
          consumer_count = direct_reply_channel.nil? ? 0_u32 : 1_u32
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, consumer_count)
        end
      elsif frame.queue_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        declare_new_queue(frame)
      end
    end

    private def redeclare_queue(frame, q)
      if q.exclusive && !exclusive_queues.includes? q
        send_resource_locked(frame, "Exclusive queue")
      elsif q.internal?
        send_access_refused(frame, "Queue '#{frame.queue_name}' in vhost '#{@vhost.name}' is internal")
      elsif frame.passive || q.match?(frame)
        unless frame.no_wait
          q.redeclare
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, q.name,
            q.message_count, q.consumer_count)
        end
      else
        send_precondition_failed(frame, "Existing queue '#{q.name}' declared with other arguments")
      end
    end

    @last_tmp_queue_name : String?

    private def declare_new_queue(frame)
      if frame.queue_name.empty?
        @last_tmp_queue_name = frame.queue_name = Queue.generate_name
      end
      dlx = frame.arguments["x-dead-letter-exchange"]?.try &.as?(String)
      dlx_ok = dlx.nil? || (@user.can_write?(@vhost.name, dlx) && @user.can_read?(@vhost.name, name))
      unless @user.can_config?(@vhost.name, frame.queue_name) && dlx_ok
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue_name}'")
        return
      end
      @vhost.apply(frame)
      if frame.exclusive
        @exclusive_queues << @vhost.queues[frame.queue_name]
      end
      unless frame.no_wait
        send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
      end
    end

    private def bind_queue(frame)
      if frame.queue_name.empty? && @last_tmp_queue_name
        frame.queue_name = @last_tmp_queue_name.not_nil!
      end
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      elsif frame.exchange_name.empty?
        send_access_refused(frame, "Not allowed to bind to the default exchange")
      elsif @vhost.queues.fetch(frame.queue_name, nil).try &.internal?
        send_access_refused(frame, "Not allowed to bind to internal queue")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      elsif frame.exchange_name.empty?
        send_access_refused(frame, "Not allowed to unbind from the default exchange")
      elsif @vhost.queues.fetch(frame.queue_name, nil).try &.internal?
        send_access_refused(frame, "Not allowed to unbind from the internal queue")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
      end
    end

    private def bind_exchange(frame)
      source = @vhost.exchanges.fetch(frame.source, nil)
      destination = @vhost.exchanges.fetch(frame.destination, nil)
      if destination.nil?
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif source.nil?
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      elsif frame.source.empty? || frame.destination.empty?
        send_access_refused(frame, "Not allowed to bind to the default exchange")
      elsif source.try(&.internal) || destination.try(&.internal)
        send_access_refused(frame, "Not allowed to bind to internal exchange")
      elsif source.try(&.persistent?)
        send_access_refused(frame, "Not allowed to bind persistent exchange to exchange")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_exchange(frame)
      source = @vhost.exchanges.fetch(frame.source, nil)
      destination = @vhost.exchanges.fetch(frame.destination, nil)
      if destination.nil?
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif source.nil?
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      elsif frame.source.empty? || frame.destination.empty?
        send_access_refused(frame, "Not allowed to unbind from the default exchange")
      elsif source.try(&.internal) || destination.try(&.internal)
        send_access_refused(frame, "Not allowed to unbind from internal exchange")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      unless @user.can_read?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        return
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        else
          messages_purged = q.purge
          send AMQP::Frame::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
        end
      else
        send_not_found(frame, "Queue '#{frame.queue_name}' not found")
      end
    end

    private def start_publish(frame)
      unless @user.can_write?(@vhost.name, frame.exchange)
        send_access_refused(frame, "User not allowed to publish to exchange '#{frame.exchange}'")
        return
      end
      with_channel frame, &.start_publish(frame)
    end

    private def consume(frame)
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
        return
      end
      with_channel frame, &.consume(frame)
    end

    private def basic_get(frame)
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
        return
      end
      with_channel frame, &.basic_get(frame)
    end
  end
end
