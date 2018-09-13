require "logger"
require "./consumer"

module AvalancheMQ
  abstract class Client
    class Channel
      class Consumer
        getter no_ack, queue, unacked, tag, exclusive
        @log : Logger

        def initialize(@channel : Client::Channel, @tag : String, @queue : Queue,
                       @no_ack : Bool, @exclusive : Bool)
          @log = @channel.log.dup
          @log.progname += " consumer=#{@tag}"
          @unacked = Deque(SegmentPosition).new(@channel.prefetch_count)
        end

        def accepts?
          @channel.prefetch_count.zero? || @unacked.size < @channel.prefetch_count
        end

        def deliver(msg, sp, queue, redelivered = false)
          @unacked << sp unless @no_ack

          @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(queue, sp, @no_ack, self)
          @log.debug { "Sending BasicDeliver" }
          deliver = AMQP::Basic::Deliver.new(@channel.id, @tag,
            delivery_tag,
            redelivered,
            msg.exchange_name, msg.routing_key)
          @channel.client.deliver(deliver, msg)
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

        def cancel
          @channel.send AMQP::Basic::Cancel.new(@channel.id, @tag, true)
        end

        def details
          channel_details = @channel.details
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

        def to_json(json : JSON::Builder)
          details.to_json(json)
        end
      end
    end
  end
end
