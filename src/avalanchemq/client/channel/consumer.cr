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
          initial_size = @channel.prefetch_count.zero? ? 1024 : @channel.prefetch_count
          @unacked = Deque(SegmentPosition).new(initial_size)
          @unack_lock = Mutex.new
        end

        def name
          @tag
        end

        def accepts?
          (@channel.prefetch_count.zero? || (@unacked.size < @channel.prefetch_count)) &&
            @channel.client_flow?
        end

        def deliver(msg, sp, redelivered = false, recover = false)
          unless @no_ack || recover
            @unack_lock.synchronize do
              @unacked << sp
            end
          end

          persistent = msg.properties.delivery_mode == 2_u8
          @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(@queue, sp,
                                                    persistent, @no_ack,
                                                    self)
          @log.debug { "Sending BasicDeliver" }
          deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
            delivery_tag,
            redelivered,
            msg.exchange_name, msg.routing_key)
          ok = @channel.client.deliver(deliver, msg)
          @channel.deliver_count += 1 if ok
          @channel.redeliver_count += 1 if ok && redelivered
          Fiber.yield if @channel.deliver_count % 8192 == 0
          ok
        end

        def ack(sp)
          @unack_lock.synchronize do
            idx = @unacked.index(sp)
            if idx
              @unacked.delete_at(idx)
              @log.debug { "Acking #{sp}. Unacked: #{@unacked.size}" }
            end
          end
        end

        def reject(sp)
          @unack_lock.synchronize do
            idx = @unacked.index(sp)
            if idx
              @unacked.delete_at(idx)
              @log.debug { "Rejecting #{sp}. Unacked: #{@unacked.size}" }
            end
          end
        end

        def recover(requeue)
          @unack_lock.synchronize do
            if requeue
              loop do
                sp = @unacked.shift? || break
                @queue.reject(sp, requeue: true)
              end
            else
              @unacked.each do |sp|
                # redeliver to the original recipient
                @queue.read(sp) do |env|
                  deliver(env.message, sp, true, recover: true)
                end
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
