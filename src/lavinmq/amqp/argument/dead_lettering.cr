require "amq-protocol"
require "../argument"
require "../argument_validator/string_validator.cr"
require "../argument_validator/dead_lettering_validator.cr"

module LavinMQ::AMQP
  module Argument
    module DeadLettering
      # Use included hook to make this being run in the including class
      macro included
        add_argument_validator "x-dead-letter-exchange", ArgumentValidator::StringValidator.new
        add_argument_validator "x-dead-letter-routing-key", ArgumentValidator::DeadLetteringValidator.new
      end

      class DeadLetterer
        property dlx : String? = nil
        property dlrk : String? = nil
        # Tracks which queues are currently in a dead-letter routing chain
        # per fiber, to prevent infinite recursion from DLX cycles (A→B→A).
        # Same pattern as Set#add? guard in Exchange#find_queues.
        @@visited_queues = Hash(Fiber, Set(String)).new

        def initialize(@vhost : VHost, @queue_name : String, @log : Logger)
        end

        private def cycle?(props, reason) : Bool
          unless headers = props.headers
            return false
          end
          unless xdeaths = headers["x-death"]?.try &.as?(Array(AMQ::Protocol::Field))
            return false
          end
          # xdeath is sorted with the newest death first. To figure out if it's a
          # cycle or not we scan until we find the current queue with the same
          # reason. If we find any death because of a reject first, it's not
          # a cycle.
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
        def route(msg : BytesMessage, reason)
          # No dead letter exchange => nothing to do
          return unless dlx = (msg.dlx || dlx())
          fiber = Fiber.current
          if visited = @@visited_queues[fiber]?
            # Already in a dead-letter chain, check for cycle
            unless visited.add?(@queue_name)
              @log.warn { "Dropping dead letter from #{@queue_name}: DLX cycle detected" }
              return
            end
            begin
              route_internal(msg, reason, dlx)
            ensure
              visited.delete(@queue_name)
            end
          else
            # Top-level entry: create visited set and own cleanup
            @@visited_queues[fiber] = Set{@queue_name}
            begin
              route_internal(msg, reason, dlx)
            ensure
              @@visited_queues.delete(fiber)
            end
          end
        end

        private def route_internal(msg, reason, dlx)
          ex = @vhost.exchanges[dlx.to_s]?.as?(AMQP::Exchange) || return

          dlrk = msg.dlrk || dlrk()

          props = create_message_properties(msg, reason)
          routing_headers = props.headers

          # If a dead lettering key exists, no routing to CC/BCC should be done
          # but the header should be maintained, so we must clone and remove them
          # for routing
          if dlrk && (rk = routing_headers.try &.clone)
            rk.delete("CC")
            rk.delete("BCC")
            routing_headers = rk
          end
          routing_rk = (dlrk || msg.routing_key).to_s

          # We're not publishing to an exchange, the queue itself is responsible
          # for delivering to destinations. This is to be able to do correct
          # cycle detection and to not create a lot of faulty stats.
          # This means that no delay, consistent hash check or such is performed
          # if the dead letter exchange has any of these features enabled.
          queues = Set(AMQP::Queue).new
          ex.find_queues(routing_rk, routing_headers, queues)
          return if queues.empty?

          is_cycle = cycle?(props, reason)

          dead_lettered_msg = Message.new(
            RoughTime.unix_ms, dlx.to_s, routing_rk.to_s,
            props, msg.bodysize, IO::Memory.new(msg.body))

          queues.each do |q|
            next if is_cycle && q.name == @queue_name
            @log.trace { "dead lettering dest=#{q.name} msg=#{dead_lettered_msg}" }
            q.publish(dead_lettered_msg)
          rescue ex
            @log.warn(exception: ex) { "Unexpected error when dead-lettering to #{q.name}" }
          end
        end

        private def create_message_properties(msg, reason)
          props = msg.properties
          h = props.headers || AMQP::Table.new
          h.reject! { |k, _| k.in?("x-dead-letter-exchange", "x-dead-letter-routing-key") }

          # there's a performance advantage to do `has_key?` over `||=`
          h["x-first-death-reason"] = reason.to_s unless h.has_key? "x-first-death-reason"
          h["x-first-death-queue"] = @queue_name unless h.has_key? "x-first-death-queue"
          h["x-first-death-exchange"] = msg.exchange_name unless h.has_key? "x-first-death-exchange"

          routing_keys = [msg.routing_key.as(AMQP::Field)]
          # Add any CC, but NOT BCC!
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
