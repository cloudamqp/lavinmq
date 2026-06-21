require "uri"
require "./errors"

module LavinMQ
  module AMQP10
    # Resolves AMQP 1.0 link addresses using RabbitMQ's "v2" address format
    # (https://www.rabbitmq.com/docs/amqp#addresses). AMQP 1.0 has no topology
    # frames, so the address string carries the exchange/routing-key or queue.
    module Address
      # Where a published message is routed: an exchange + routing key. A target
      # of `/queues/:q` maps to the default exchange with the queue as routing
      # key (the default exchange delivers straight to the same-named queue).
      record Target, exchange : String, routing_key : String

      # `nil` target means anonymous: routing comes from each message's `to`.
      def self.parse_target(address : String?) : Target?
        return nil if address.nil? || address.empty?
        segments = split(address)
        if segments[0]? == "exchanges" && segments.size == 3
          Target.new(decode(segments[1]), decode(segments[2]))
        elsif segments[0]? == "exchanges" && segments.size == 2
          Target.new(decode(segments[1]), "")
        elsif segments[0]? == "queues" && segments.size == 2
          Target.new("", decode(segments[1]))
        else
          raise Error.new("Unsupported target address #{address.inspect}")
        end
      end

      # Source address for a consuming (sender) link: only `/queues/:q`.
      def self.parse_source(address : String?) : String
        raise Error.new("Missing source address") if address.nil? || address.empty?
        segments = split(address)
        if segments[0]? == "queues" && segments.size == 2
          decode(segments[1])
        else
          raise Error.new("Unsupported source address #{address.inspect} (expected /queues/:queue)")
        end
      end

      # Split a "/a/b/c" path into its non-empty segments, preserving order.
      private def self.split(address : String) : Array(String)
        address.lchop('/').split('/')
      end

      private def self.decode(s : String) : String
        URI.decode(s)
      end
    end
  end
end
