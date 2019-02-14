require "logger"
require "../../sortable_json"

module AvalancheMQ
  abstract class Client
    class Channel
      class Consumer
        include SortableJSON
        getter no_ack, queue, unacked, tag, exclusive
        @log : Logger

        def initialize(@channel : Client::Channel, @tag : String, @queue : Queue,
                       @no_ack : Bool, @exclusive : Bool)
          @log = @channel.log.dup
          @log.progname += " consumer=#{@tag}"
          @unacked = Deque(SegmentPosition).new(@channel.prefetch_count)
        end

        def name
          @tag
        end

        def accepts?
          @channel.prefetch_count.zero? || @unacked.size < @channel.prefetch_count
        end

        def deliver(msg, sp, redelivered = false)
          @unacked << sp unless @no_ack

          @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(@queue, sp, @no_ack, self)
          @log.debug { "Sending BasicDeliver" }
          deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
            delivery_tag,
            redelivered,
            msg.exchange_name, msg.routing_key)
          ok = @channel.client.deliver(deliver, msg)
          @channel.deliver_count += 1 if ok
          @channel.redeliver_count += 1 if ok && redelivered
          ok
        end

        def ack(sp)
          idx = @unacked.index(sp)
          if idx
            @unacked.delete_at(idx)
            @log.debug { "Acking #{sp}. Unacked: #{@unacked.size}" }
          end
        end

        def reject(sp)
          idx = @unacked.index(sp)
          if idx
            @unacked.delete_at(idx)
            @log.debug { "Rejecting #{sp}. Unacked: #{@unacked.size}" }
          end
        end

        def recover(requeue)
          @unacked.each do |sp|
            if requeue
              @queue.reject(sp, requeue: true)
            else
              # redeliver to the original recipient
              @queue.read_message(sp) do |env|
                deliver(env.message, sp, redelivered: true)
              end
            end
          end
        end

        def cancel
          @channel.send AMQP::Frame::Basic::Cancel.new(@channel.id, @tag, true)
        end

        def details_tuple
          channel_details = @channel.details_tuple
          {
            queue: {
              name:  @queue.name,
              vhost: @queue.vhost,
            },
            consumer_tag:    @tag,
            exclusive:       @exclusive,
            ack_required:    !@no_ack,
            prefetch_count:  @channel.prefetch_count,
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
