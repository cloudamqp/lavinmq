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

        def close
          @closed = true
        end

        @flow_channel = ::Channel(Bool).new(1)

        def flow(active : Bool)
          @flow = active
          select
          when @flow_channel.send active
          end
        end

        private def deliver_loop
          loop do
            break if @closed
            wait_until_accepts
            @log.debug { "Waiting for msg or channel flow change" }
            select
            when sp = @queue.to_deliver.receive?
              break if sp.nil?
              @log.debug { "Got SP #{sp}" }
              if env = @queue.get_msg(sp, self)
                begin
                  deliver(env.message, sp, env.redelivered)
                rescue ex
                  @queue.ready.insert(sp)
                  raise ex
                end
              end
            when @flow_channel.receive
              @log.debug { "Channel flow controlled while waiting for msgs" }
              next
            end
          end
        ensure
          @log.debug { "deliver loop exiting" }
        end

        def name
          @tag
        end

        # blocks until the consumer can accept more messages
        private def wait_until_accepts
          ch = @channel
          until @flow
            @log.debug { "Waiting for client flow" }
            case @flow_channel.receive?
            when true  then break
            when false then next
            else            return
            end
          end
          return true if @prefetch_count.zero?
          @log.debug { "Waiting for prefetch capacity" }
          if ch.global_prefetch?
            until ch.consumers.sum(&.unacked) < ch.prefetch_count
              sleep 0.1 # FIXME: terribly inefficent
            end
          else
            until @unacked < @prefetch_count
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

        @has_capacity = ::Channel(Nil).new

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
            select
            when @has_capacity.send(nil)
            else
            end
          end
          @unacked -= 1
        end

        def reject(sp)
          if @unacked == @prefetch_count
            select
            when @has_capacity.send(nil)
            else
            end
          end
          @unacked -= 1
        end

        def cancel
          @channel.send AMQP::Frame::Basic::Cancel.new(@channel.id, @tag, true)
          @channel.consumers.delete self
          @closed = true
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
