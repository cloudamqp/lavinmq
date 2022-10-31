require "log"
require "../../sortable_json"

module LavinMQ
  class Client
    class Channel
      class Consumer
        include SortableJSON

        getter no_ack, queue, unacked, tag, exclusive, channel, priority
        getter prefetch_count = 0u16

        @log : Log
        @unacked = 0_u32
        @prefetch_count : UInt16
        @closed = false
        @flow : Bool

        def initialize(@channel : Client::Channel, @tag : String,
                       @queue : Queue, @no_ack : Bool, @exclusive : Bool, @priority : Int32)
          @prefetch_count = @channel.prefetch_count
          @flow = @channel.flow?
          @log = @channel.log.for "consumer=#{@tag}"
          spawn deliver_loop, name: "Consumer deliver loop"
        end

        def close(basic_cancel = false)
          @closed = true
          @queue.rm_consumer(self, basic_cancel)
          @has_capacity.close
          @flow_change.close
        end

        @flow_change = ::Channel(Bool).new

        def flow(active : Bool)
          @flow = active
          @flow_change.try_send? active
        end

        private def deliver_loop
          loop do
            break if @closed
            wait_until_accepts
            wait_for_messages
            get_and_deliver_message
          rescue ::Channel::ClosedError
            break
          end
        ensure
          @log.debug { "deliver loop exiting" }
        end

        private def get_and_deliver_message
          @log.debug { "Getting a new message" }
          @queue.get_msg(self) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
        end

        private def wait_for_messages : Bool
          loop do
            return true if @flow && !@queue.paused? && !@queue.ready.empty?
            @log.debug { "Waiting for msg or queue/channel flow change" }
            select
            when is_flow = @flow_change.receive
              @log.debug { "Channel flow=#{is_flow}" }
            when is_empty = @queue.ready.empty_change.receive
              @log.debug { "Queue is #{is_empty ? "" : "not"} empty" }
            when is_paused = @queue.paused_change.receive
              @log.debug { "Queue is #{is_paused ? "" : "not"} paused" }
            end
          end
        end

        def name
          @tag
        end

        # blocks until the consumer can accept more messages
        private def wait_until_accepts : Nil
          return if @prefetch_count.zero?
          ch = @channel
          if ch.global_prefetch?
            @log.debug { "Waiting for global prefetch capacity" }
            until ch.consumers.sum(&.unacked) < ch.prefetch_count
              ::Channel.receive_first(ch.consumers.map(&.has_capacity))
            end
          else
            until @unacked < @prefetch_count
              @log.debug { "Waiting for prefetch capacity" }
              @has_capacity.receive
            end
          end
        end

        def accepts?
          ch = @channel
          return false unless @flow
          return true if @prefetch_count.zero?
          if ch.global_prefetch?
            ch.consumers.sum(&.unacked) < ch.prefetch_count
          else
            @unacked < @prefetch_count
          end
        end

        getter has_capacity = ::Channel(Nil).new

        private def deliver(msg, sp, redelivered = false, recover = false)
          @unacked += 1 unless @no_ack || recover
          persistent = msg.properties.delivery_mode == 2_u8
          # @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(@queue, sp, persistent, @no_ack, self)
          # @log.debug { "Sending BasicDeliver" }
          deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
            delivery_tag,
            redelivered,
            msg.exchange_name, msg.routing_key)
          @channel.deliver(deliver, msg, redelivered)
        end

        def ack(sp)
          if @unacked == @prefetch_count
            @has_capacity.try_send? nil
          end
          @unacked -= 1
        end

        def reject(sp)
          if @unacked == @prefetch_count
            @has_capacity.try_send? nil
          end
          @unacked -= 1
        end

        def cancel
          @channel.send AMQP::Frame::Basic::Cancel.new(@channel.id, @tag, true)
          @channel.consumers.delete self
          close
        end

        def details_tuple
          channel_details = @channel.details_tuple
          {
            queue: {
              name:  @queue.name,
              vhost: @queue.vhost.name,
            },
            consumer_tag:    @tag,
            exclusive:       @exclusive,
            ack_required:    !@no_ack,
            prefetch_count:  prefetch_count,
            priority:        @priority,
            channel_details: {
              peer_host:       channel_details[:connection_details][:peer_host]?,
              peer_port:       channel_details[:connection_details][:peer_port]?,
              connection_name: channel_details[:connection_details][:name],
              user:            channel_details[:user],
              number:          channel_details[:number],
              name:            channel_details[:name],
            },
          }
        end
      end
    end
  end
end
