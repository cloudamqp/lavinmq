require "../argument"
require "../argument_validator/int_validator"

module LavinMQ::AMQP
  module Argument
    module Retry
      macro included
        VALIDATOR_RETRY_INT = ArgumentValidator::IntValidator.new(min_value: 1)

        add_argument_validator "x-retry-max-count", VALIDATOR_RETRY_INT
        add_argument_validator "x-retry-delay", VALIDATOR_RETRY_INT
        add_argument_validator "x-retry-delay-multiplier", VALIDATOR_RETRY_INT
        add_argument_validator "x-retry-max-delay", VALIDATOR_RETRY_INT
      end

      class Retrier
        getter? enabled = false
        getter retry_queues : Array(Queue)?

        @max_count : Int64 = 0
        @delay : Int64 = 1000
        @delay_multiplier : Int64 = 2
        @max_delay : Int64 = 60000

        def initialize(@vhost : VHost, @queue_name : String, @log : Logger)
        end

        def handle_arguments(arguments : AMQP::Table, effective_args : Array(String), durable : Bool)
          max_count = arguments["x-retry-max-count"]?.try(&.as?(Int)).try(&.to_i64)
          return unless max_count
          @enabled = true
          @max_count = max_count
          effective_args << "x-retry-max-count"
          @delay = arguments["x-retry-delay"]?.try(&.as?(Int)).try(&.to_i64) || 1000i64
          effective_args << "x-retry-delay" if arguments["x-retry-delay"]?
          @delay_multiplier = arguments["x-retry-delay-multiplier"]?.try(&.as?(Int)).try(&.to_i64) || 2i64
          effective_args << "x-retry-delay-multiplier" if arguments["x-retry-delay-multiplier"]?
          @max_delay = arguments["x-retry-max-delay"]?.try(&.as?(Int)).try(&.to_i64) || 60000i64
          effective_args << "x-retry-max-delay" if arguments["x-retry-max-delay"]?
          init_retry_queues(durable)
        end

        # Builds a retry message if retries remain, returns nil if exhausted.
        # Caller is responsible for publishing and deleting the original.
        def retry(msg : BytesMessage) : {Queue, Message}?
          return unless @enabled
          retry_queues = @retry_queues || return
          headers = msg.properties.headers || AMQP::Table.new
          retry_count = headers["x-retry-count"]?.try(&.as?(Int)).try(&.to_i64) || 0i64
          return if retry_count >= @max_count
          level = Math.min(retry_count, retry_queues.size.to_i64 - 1)
          headers["x-retry-count"] = retry_count + 1
          props = msg.properties
          props.headers = headers
          target = retry_queues[level]
          retry_msg = Message.new(
            msg.timestamp, "", target.name,
            props, msg.bodysize, IO::Memory.new(msg.body))
          {target, retry_msg}
        end

        def delete_retry_queues
          if rqs = @retry_queues
            rqs.each do |rq|
              @vhost.queues.delete(rq.name)
              rq.delete
            end
            @retry_queues = nil
          end
        end

        private def init_retry_queues(durable : Bool)
          queues = Array(Queue).new(@max_count.to_i)
          @max_count.times do |level|
            ttl = Math.min(@delay * (@delay_multiplier ** level), @max_delay)
            rq = RetryQueue.create(@vhost, @queue_name, level.to_i, ttl, durable)
            @vhost.queues[rq.name] = rq
            queues << rq
          end
          @retry_queues = queues
        end
      end
    end
  end
end
