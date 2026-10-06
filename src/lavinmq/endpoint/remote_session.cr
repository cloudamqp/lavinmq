require "amqp-client"
require "./session"
require "../version"

module LavinMQ
  module Endpoint
    # A session with a broker reached over AMQP, see `Session`
    class RemoteSession < Session
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?
      @prefetch = 0_u16
      @closing = false

      def initialize(@uri : URI, name : String)
        super(name)
      end

      def open : Nil
        uri = @uri.dup
        params = uri.query_params
        params["name"] ||= @name
        params["product"] = "LavinMQ"
        params["product_version"] = LavinMQ::VERSION.to_s
        uri.query = params.to_s
        @closing = false
        conn = ::AMQP::Client.new(uri).connect
        conn.on_close do |code, reason|
          notify_closed("#{code} - #{reason}") unless @closing
        end
        @conn = conn
        @ch = conn.channel
        next_generation
      end

      def close : Nil
        @closing = true
        @conn.try &.close(no_wait: false)
      rescue ::AMQP::Client::Error | IO::Error
      ensure
        @ch = nil
      end

      def closed? : Bool
        @conn.nil? || @conn.try(&.closed?) || false
      end

      def declare_queue(name : String, passive : Bool, durable = true, auto_delete = false,
                        args = AMQ::Protocol::Table.new) : Tuple(String, UInt32)
        q = with_channel(&.queue_declare(name, passive: passive, durable: durable,
          exclusive: false, auto_delete: auto_delete, args: args))
        {q[:queue_name], q[:message_count]}
      end

      def declare_exchange(name : String, type : String, passive : Bool,
                           args = AMQ::Protocol::Table.new) : Nil
        with_channel(&.exchange_declare(name, type, passive: passive, args: args))
      end

      def delete_queue(name : String) : Nil
        with_channel(&.queue_delete(name))
      end

      def delete_exchange(name : String) : Nil
        with_channel(&.exchange_delete(name))
      end

      def bind_queue(queue : String, exchange : String, routing_key : String,
                     args = AMQ::Protocol::Table.new) : Nil
        with_channel(&.queue_bind(queue, exchange, routing_key, args: args))
      end

      def bind_exchange(destination : String, source : String, routing_key : String,
                        args = AMQ::Protocol::Table.new) : Nil
        with_channel(&.exchange_bind(source, destination, routing_key, args: args))
      end

      def unbind_exchange(destination : String, source : String, routing_key : String,
                          args = AMQ::Protocol::Table.new) : Nil
        with_channel(&.exchange_unbind(source, destination, routing_key, args: args))
      end

      def prefetch=(count : UInt16)
        @prefetch = count
        channel.prefetch(count)
      end

      def consume(queue : String, tag : String, no_ack : Bool, exclusive : Bool,
                  args : AMQ::Protocol::Table, &blk : Delivery -> Nil) : Nil
        with_channel do |ch|
          ch.basic_consume(queue, tag: tag, no_ack: no_ack, exclusive: exclusive,
            block: true, args: args) do |msg|
            blk.call Delivery.new(msg.delivery_tag, msg.exchange, msg.routing_key,
              msg.properties, msg.body_io.to_slice, msg.redelivered)
          end
        end
      end

      def cancel(tag : String) : Nil
        ch = @ch || return
        return if ch.closed?
        ch.basic_cancel(tag, no_wait: true)
      rescue ::AMQP::Client::Error | IO::Error
      end

      def ack(delivery_tag : UInt64, multiple = false) : Nil
        ch = @ch || return
        return if ch.closed?
        ch.basic_ack(delivery_tag, multiple: multiple)
      end

      def reject(delivery_tag : UInt64, requeue : Bool) : Nil
        ch = @ch || return
        return if ch.closed?
        ch.basic_reject(delivery_tag, requeue: requeue)
      end

      def publish(exchange : String, routing_key : String,
                  properties : AMQ::Protocol::Properties, body : Bytes) : Nil
        channel.basic_publish(body, exchange, routing_key, props: properties)
      end

      def publish(exchange : String, routing_key : String,
                  properties : AMQ::Protocol::Properties, body : Bytes,
                  &on_confirm : Bool -> Nil) : Nil
        channel.basic_publish(body, exchange, routing_key, props: properties) do |ok|
          on_confirm.call(ok)
        end
      end

      private def channel : ::AMQP::Client::Channel
        @ch || raise ClosedError.new("Session #{@name} not open")
      end

      # A failed operation closes an AMQP channel. Reopen it so the session
      # stays usable (a passive declare is how existence is checked), and
      # translate the error.
      private def with_channel(&)
        ch = channel
        begin
          yield ch
        rescue ex : ::AMQP::Client::Channel::ClosedException
          conn = @conn
          raise translate(ex) if conn.nil? || conn.closed?
          @ch = new_ch = conn.channel
          new_ch.prefetch(@prefetch) unless @prefetch.zero?
          raise translate(ex)
        rescue ex : ::AMQP::Client::Connection::ClosedException
          raise ClosedError.new(ex.message, cause: ex)
        end
      end

      private def translate(ex : Exception) : Exception
        message = ex.message.to_s
        case message
        when .starts_with?("404") then NotFound.new(message, cause: ex)
        when .starts_with?("403"), .starts_with?("405"), .starts_with?("406")
          Refused.new(message, cause: ex)
        else ClosedError.new(message, cause: ex)
        end
      end
    end
  end
end
