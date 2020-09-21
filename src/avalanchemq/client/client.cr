require "logger"
require "openssl"
require "socket"
require "../vhost"
require "../message"
require "./channel"
require "../user"
require "../stats"
require "../sortable_json"
require "../rough_time"
require "../error"
require "./amqp_connection"

module AvalancheMQ
  class Client
    include Stats
    include SortableJSON

    property direct_reply_consumer_tag
    getter vhost, channels, log, exclusive_queues, name
    getter user
    getter remote_address
    getter max_frame_size : UInt32
    getter channel_max : UInt16
    getter heartbeat : UInt16
    getter auth_mechanism : String
    getter client_properties : AMQP::Table
    getter direct_reply_consumer_tag : String?
    getter log : Logger

    @connected_at : Int64
    @running = true
    @last_heartbeat = RoughTime.utc
    rate_stats(%w(send_oct recv_oct channel_created channel_closed))

    def initialize(@socket : TCPSocket | OpenSSL::SSL::Socket | UNIXSocket,
                   @remote_address : Socket::IPAddress,
                   @local_address : Socket::IPAddress,
                   @vhost : VHost,
                   @user : User,
                   tune_ok,
                   start_ok)
      @log = vhost.log.dup
      @log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      @name = "#{@remote_address} -> #{@local_address}"
      @client_properties = start_ok.client_properties
      @connected_at = Time.utc.to_unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @exclusive_queues = Array(Queue).new
      @vhost.add_connection(self)
      @log.debug "Connected"
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def channel_name_prefix
      @remote_address.to_s
    end

    def details_tuple
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @channel_max,
        timeout:           @heartbeat,
        client_properties: @client_properties,
        vhost:             @vhost.name,
        user:              @user.name,
        protocol:          "AMQP 0-9-1",
        auth_mechanism:    @auth_mechanism,
        host:              @local_address.address,
        port:              @local_address.port,
        peer_host:         @remote_address.address,
        peer_port:         @remote_address.port,
        name:              @name,
        ssl:               @socket.is_a?(OpenSSL::SSL::Socket),
        state:             state,
      }.merge(stats_details)
    end

    private def read_loop
      i = 0
      loop do
        AMQP::Frame.from_io(@socket) do |frame|
          @log.debug { "Received #{frame.inspect}" }
          if (i += 1) == 8192
            i = 0
            Fiber.yield
          end
          if @running
            process_frame(frame)
          else
            case frame
            when AMQP::Frame::Connection::Close, AMQP::Frame::Connection::CloseOk
              process_frame(frame)
            when AMQP::Frame::Body
              @log.debug { "Skipping body, waiting for Close(Ok)" }
              frame.body.skip(frame.body_size)
              true
            else
              @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
              true
            end
          end
        end || break
      rescue IO::TimeoutError
        send_heartbeat || break
      end
    rescue frame : AMQP::Error::FrameDecode
      @log.error { frame.inspect }
      send AMQP::Frame::Connection::Close.new(501_u16, "FRAME_ERROR", 0_u16, 0_u16)
      false
    rescue ex : IO::Error | OpenSSL::SSL::Error | ::Channel::ClosedError
      @log.debug { "Lost connection, while reading (#{ex.inspect})" } unless closed?
      cleanup
    rescue ex : Exception
      @log.error { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    ensure
      @running = false
    end

    private def send_heartbeat
      if @last_heartbeat + @heartbeat.seconds < RoughTime.utc
        send(AMQP::Frame::Heartbeat.new)
      else
        true
      end
    end

    def send(frame : AMQP::Frame)
      return false if closed?
      @log.debug { "Send #{frame.inspect}" }
      @write_lock.synchronize do
        @socket.write_bytes frame, IO::ByteFormat::NetworkEndian
        @socket.flush
      end
      @send_oct_count += 8_u64 + frame.bytesize
      case frame
      when AMQP::Frame::Connection::CloseOk
        @log.debug "Disconnected"
        cleanup
        false
      else
        true
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost connection, while sending (#{ex.inspect})" } unless closed?
      cleanup
      false
    rescue ex : IO::TimeoutError
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    @write_lock = Mutex.new(:unchecked)

    def deliver(frame, msg)
      @write_lock.synchronize do
        #@log.debug { "Send #{frame.inspect}" }
        @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + frame.bytesize
        header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
        #@log.debug { "Send #{header.inspect}" }
        @socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + header.bytesize
        pos = 0
        while pos < msg.size
          length = Math.min(msg.size - pos, @max_frame_size - 8).to_u32
          #@log.debug { "Send BodyFrame (pos #{pos}, length #{length})" }
          body = case msg
                 in BytesMessage
                   AMQP::Frame::BytesBody.new(frame.channel, length, msg.body[pos, length])
                 in Message
                   AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
                 end
          @socket.write_bytes body, ::IO::ByteFormat::NetworkEndian
          @send_oct_count += 8_u64 + body.bytesize
          pos += length
        end
        #@log.debug { "Flushing" }
        @socket.flush
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | AMQ::Protocol::Error::FrameEncode
      @log.debug { "Lost connection, while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex : IO::TimeoutError
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Delivery exception: #{ex.inspect_with_backtrace}" }
      raise ex
    end

    protected def cleanup
      super
      begin
        @socket.close unless @socket.closed?
      rescue ex
        @log.debug { "error when closing socket: #{ex.inspect_with_backtrace}" }
      end
    end
    def state
      !@running ? "closed" : (@vhost.flow? ? "running" : "flow")
    end

    def self.start(socket, remote_address, local_address, vhosts, users, log)
      AMQPConnection.start(socket, remote_address, local_address, vhosts, users, log.dup)
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
      @channel_created_count += 1
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
        @channel_closed_count += 1
        @channels.delete(frame.channel).try &.close
        send AMQP::Frame::Channel::CloseOk.new(frame.channel)
      when AMQP::Frame::Channel::CloseOk
        @channel_closed_count += 1
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
    rescue frame : AMQP::Error::FrameDecode
      @log.error { frame.inspect }
      send AMQP::Frame::Connection::Close.new(501_u16, "FRAME_ERROR", 0_u16, 0_u16)
      false
    rescue frame : Error::UnexpectedFrame
      @log.error { "#{frame.inspect}, unexpected frame" }
      close_channel(frame, 505_u16, "UNEXPECTED_FRAME")
      true
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      close_channel(ex, 540_u16, "NOT_IMPLEMENTED")
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
      @channel_closed_count += @channels.size
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
      return close_connection(frame, code, text) if frame.channel.zero?
      case frame
      when AMQP::Frame::Header, AMQP::Frame::Body
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, 0, 0)
      else
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, frame.class_id, frame.method_id)
      end
      @channel_closed_count += 1
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
      @channel_closed_count += @channels.size
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
          send_resource_locked(frame, "Existing exchange '#{name}' declared with other arguments")
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
      if frame.exchange_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
        return
      elsif !@user.can_config?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
      else
        @vhost.apply(frame)
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
        send_resource_locked(frame, "Existing queue '#{q.name}' declared with other arguments")
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
