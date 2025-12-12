require "amq-protocol"
require "../../argument_validator/string_validator.cr"
require "../../argument_validator/dead_lettering_validator.cr"

module LavinMQ::AMQP
  module ArgumentTarget
    macro included
      ARGUMENT_VALIDATORS = Hash(String, ArgumentValidator).new

      def self.add_argument_validator(key, validator)
        ARGUMENT_VALIDATORS[key] = validator
      end

      def self.validate_arguments!(arguments : AMQP::Table)
        arguments.each do |k, v|
          if validator = ARGUMENT_VALIDATORS[k]?
            validator.validate!(k, v, arguments)
          end
        end
      end
    end
  end

  class Queue < LavinMQ::Queue
    module Feature
      module DeadLettering
        macro included
          add_argument_validator "x-dead-letter-exchange", ArgumentValidator::StringValidator.new
          add_argument_validator "x-dead-letter-routing-key", ArgumentValidator::DeadLetteringValidator.new
        end

        class DeadLetterer
          property dlx : String? = nil
          property dlrk : String? = nil

          def initialize(@vhost : VHost, @queue_name : String, @log : Logger)
          end

          def cycle?(props, reason) : Bool
            unless (headers = props.headers)
              return false
            end
            unless (xdeaths = headers["x-death"]?.try &.as?(Array(AMQ::Protocol::Field)))
              return false
            end
            # xdeath is sorted with the newest death first. To figure out if it's a
            # cycle or not we scan until we found this queue. If we find any death
            # because of a reject first, it's no a cycle.
            xdeaths.each do |field|
              next unless xdeath = field.as?(AMQ::Protocol::Table)
              return false if xdeath["reason"]? == "rejected"
              return true if xdeath["queue"]? == @queue_name && xdeath["reason"] == reason.to_s
            end
            false
          end

          # This method will publish direct to queues instead of to the DLX.
          # It's done like this to be able to dead letter to all destinations
          # except to the queue itself if a cycle is detected.
          # This is also how it's done in rabbitmq
          def maybe_publish(msg : BytesMessage, reason)
            # No dead letter exchange => nothing to do
            return unless dlx = (msg.dlx || dlx())
            dlrk = msg.dlrk || dlrk() || msg.routing_key

            props = create_message_properties(msg, reason)

            ex = @vhost.exchanges[dlx.to_s]? || return
            queues = Set(LavinMQ::Queue).new
            ex.find_queues(dlrk, props.headers, queues)

            is_cycle = cycle?(props, reason)

            dead_lettered_msg = Message.new(
              RoughTime.unix_ms, dlx.to_s, dlrk.to_s,
              props, msg.bodysize, IO::Memory.new(msg.body))

            queues.each do |q|
              next if is_cycle && q.name == @queue_name
              @log.trace { "dead lettering dest=#{q.name} msg=#{dead_lettered_msg}" }
              q.publish(dead_lettered_msg)
            end
          end

          def create_message_properties(msg, reason)
            props = msg.properties
            h = props.headers || AMQP::Table.new
            h.reject! { |k, _| k.in?("x-dead-letter-exchange", "x-dead-letter-routing-key") }

            # there's a performance advantage to do `has_key?` over `||=`
            h["x-first-death-reason"] = reason.to_s unless h.has_key? "x-first-death-reason"
            h["x-first-death-queue"] = @queue_name unless h.has_key? "x-first-death-queue"
            h["x-first-death-exchange"] = msg.exchange_name unless h.has_key? "x-first-death-exchange"

            routing_keys = [msg.routing_key.as(AMQP::Field)]
            if cc = h["CC"]?.try(&.as?(Array(AMQP::Field)))
              routing_keys.concat cc.as(Array(AMQP::Field))
            end

            props.headers = update_x_death(h, msg.exchange_name, routing_keys, reason, msg.properties.expiration)
            props.expiration = nil
            props
          end

          private def update_x_death(headers, exchange_name, routing_keys, reason, expiration) : AMQP::Table
            xdeaths = headers["x-death"]?.as?(Array(AMQP::Field)) || Array(AMQP::Field).new(1)

            found_at = -1
            xdeaths.each_with_index do |xd, idx|
              next unless xd = xd.as?(AMQP::Table)
              next if xd["reason"]? != reason.to_s
              next if xd["queue"]? != @queue_name
              count = xd["count"].as?(Int) || 0
              xd.merge!({
                count:          count + 1,
                time:           RoughTime.utc,
                "routing-keys": routing_keys,
              })
              xd["original-expiration"] = expiration if expiration
              found_at = idx
              break
            end

            case found_at
            when -1 # not found so inserting new x-death
              death = AMQP::Table.new({
                "queue":        @queue_name,
                "reason":       reason.to_s,
                "exchange":     exchange_name,
                "count":        1,
                "time":         RoughTime.utc,
                "routing-keys": routing_keys,
              })
              death["original-expiration"] = expiration if expiration
              xdeaths.unshift death
            when 0
              # do nothing, updated xd is in the front
            else
              # move updated xd to the front
              xd = xdeaths.delete_at(found_at)
              xdeaths.unshift xd
            end
            headers["x-death"] = xdeaths
            headers
          end
        end
      end
    end
  end
end
