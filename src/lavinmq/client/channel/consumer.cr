require "log"
require "../../message"
require "../../sortable_json"

module LavinMQ
  class Client
    class Channel
      class Consumer
        include SortableJSON

        getter no_ack, queue, unacked, tag, exclusive, channel, priority

        @log : Log
        @unacked = 0_u32

        def initialize(@channel : Client::Channel, @tag : String,
                       @queue : Queue, @no_ack : Bool, @exclusive : Bool, @priority : Int32)
          @log = @channel.log.for "consumer=#{@tag}"
          spawn deliver_loop, name: "Deliver loop #{@tag}"
        end

        def name
          @tag
        end

        def prefetch_count
          @channel.prefetch_count
        end

        def accepts?
          ch = @channel
          return false unless ch.client_flow?
          return true if prefetch_count.zero?
          unacked = ch.global_prefetch? ? ch.consumers.sum(&.unacked) : @unacked
          unacked < prefetch_count
        end

        @deliveries = ::Channel(Tuple(Envelope, UInt64)).new

        def close
          @deliveries.close
        end

        def deliver_loop
          loop do
            env, delivery_tag = @deliveries.receive? || break
            # @log.debug { "Sending BasicDeliver" }
            frame = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
              delivery_tag,
              env.redelivered,
              env.message.exchange_name, env.message.routing_key)
            begin
              @channel.deliver(frame, env.message, env.redelivered)
              if @no_ack
                @queue.delete_message(env.segment_position)
              else
                @queue.unacked.push(env.segment_position, self)
              end
            rescue ex
              @log.debug { "Delivery failed, returning #{env.segment_position} to ready" }
              @queue.ready.insert(env.segment_position)
              raise ex
            end
          end
        end

        def deliver(env : Envelope, recover = false)
          unless @no_ack || recover
            @unacked += 1
          end

          persistent = env.message.properties.delivery_mode == 2_u8
          # @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(@queue, env.segment_position, persistent, @no_ack, self)
          @deliveries.send({env, delivery_tag})
        end

        def ack(sp)
          @unacked -= 1
        end

        def reject(sp)
          @unacked -= 1
        end

        def recover(requeue)
          @channel.recover(self) do |sp|
            if requeue
              reject(sp)
              @queue.reject(sp, requeue: true)
            else
              # redeliver to the original recipient
              env = @queue.read(sp)
              deliver(env, recover: true)
            end
          end
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
