require "amqp-client"

module AvalancheMQ
  module Federation
    class Upstream
      class Consumer
        def initialize(@upstream : Upstream, @queue : String)
          @log = @upstream.log.dup
          @log.progname += " consumer"
        end

        def self.start(uri)
          ::AMQP::Client.start(@u)
          set_prefetch
          consume
          spawn(read_loop, name: "Upstream consumer #{@upstream.uri.host}#read_loop")
        end

        private def read_loop
          loop do
            AMQP::Frame.from_io(@socket) do |frame|
              # @log.debug { "Read socket #{frame.inspect}" }
              case frame
              when AMQP::Frame::Basic::Deliver, AMQP::Frame::Header, AMQP::Frame::Body
                @on_frame.try &.call(frame)
                true
              when AMQP::Frame::Basic::Cancel
                unless frame.no_wait
                  write AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
                end
                write AMQP::Frame::Connection::Close.new(320_u16, "Consumer cancelled", 0_u16, 0_u16)
                true
              when AMQP::Frame::Connection::Close
                write AMQP::Frame::Connection::CloseOk.new
                false
              when AMQP::Frame::Connection::CloseOk
                false
              when AMQP::Frame::Channel::Flow
                write AMQP::Frame::Channel::FlowOk.new frame.channel, frame.active
                true
              when AMQP::Frame::Queue::BindOk, AMQP::Frame::Queue::UnbindOk
                true
              else
                raise UnexpectedFrame.new(frame)
              end
            end || break
          end
        rescue ex : IO::Error | AMQP::Error::FrameDecode
          @log.info "Consumer closed due to: #{ex.inspect}"
        ensure
          @log.debug "Closing socket"
          @socket.close rescue nil
        end

        def forward(frame)
          # @log.debug { "Read internal #{frame.inspect}" }
          case frame
          when AMQP::Frame::Basic::Ack
            unless @upstream.ack_mode == AckMode::NoAck
              write frame
            end
          when AMQP::Frame::Basic::Reject
            write frame
          else
            @log.warn { "Unexpected frame: #{frame.inspect}" }
          end
        end

        private def set_prefetch
          write AMQP::Frame::Basic::Qos.new(1_u16, 0_u32, @upstream.prefetch, false)
          AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::QosOk) }
        end

        private def consume
          write AMQP::Frame::Queue::Declare.new(1_u16, 0_u16, @queue, true,
            false, true, true, false, AMQP::Table.new)
          frame = AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Queue::DeclareOk) }
          queue = frame.queue_name
          no_ack = @upstream.ack_mode == AckMode::NoAck
          write AMQP::Frame::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
            false, no_ack, false, false, AMQP::Table.new)
          AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::ConsumeOk) }
        end

        def ack(delivery_tag)
          write AMQP::Frame::Basic::Ack.new(1_u16, delivery_tag, false)
        end

        def reject(delivery_tag)
          write AMQP::Frame::Basic::Reject.new(1_u16, delivery_tag.to_u64, false)
        end

        def init_exchange_federation(upstream_exchange, federated_exchange)
          args = AMQP::Table.new(federated_exchange.arguments)
          write AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, upstream_exchange,
            federated_exchange.type, true, true, true, true, false, args)
          read_frame { |f| f.as?(AMQP::Frame::Exchange::DeclareOk) }
          args = AMQP::Table.new({"x-downstream-name"  => System.hostname,
                                  "x-internal-purpose" => "federation",
                                  "x-max-hops"         => @upstream.max_hops})
          write AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, @upstream_q,
            "x-federation-upstream", false, true, true, true, false, args)
          read_frame { |f| f.as?(AMQP::Frame::Exchange::DeclareOk) }
          args = AMQP::Table.new({"x-internal-purpose" => "federation"})
          write AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, @upstream_q, false, true,
            false, false, false, args)
          read_frame { |f| f.as?(AMQP::Frame::Queue::DeclareOk) }
          federated_exchange.bindings_details.each do |binding|
            bind(upstream_exchange, @upstream_q, binding.key[0], binding.key[1])
          end
        end

        def cleanup_exchange_federation()

        def bind(source, destination, routing_key, arguments)
          args = AMQP::Table.new(arguments) || AMQP::Table.new
          write AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, destination,
            source, routing_key, false, args)
        end

        def unbind(source, destination, routing_key, arguments)
          args = AMQP::Table.new(arguments) || AMQP::Table.new
          write AMQP::Frame::Queue::Unbind.new(0_u16, 0_u16, destination,
            source, routing_key, args)
        end
      end
    end
  end
end
