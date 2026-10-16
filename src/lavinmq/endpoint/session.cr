require "uri"
require "json"
require "amq-protocol"

module LavinMQ
  # An Endpoint is one end of a shovel or federation link: a broker that
  # messages are consumed from or published to. A `Session` is an open
  # conversation with it, comparable to an AMQP connection with one channel.
  #
  # There are two kinds, chosen by the URI (see `Endpoint.open`):
  #
  # - `LocalSession`: a URI without host, `amqp://` or `amqp:///vhost`, is this
  #   broker. It runs in-process against the vhost: no socket, no framing, no
  #   user. Such a URI is authorized when the parameter is created
  #   (Shovel::Store.validate_config!, Federation::UpstreamStore.validate_config!).
  # - `RemoteSession`: any URI with a host, localhost included, is a broker
  #   reached over AMQP with amqp-client, using the URI's own credentials.
  module Endpoint
    class Error < Exception; end

    # A queue or exchange that doesn't exist (AMQP 404)
    class NotFound < Error; end

    # The session was closed, by #close, by the peer, or because the queue it
    # consumed from was closed or deleted
    class ClosedError < Error; end

    # The broker refused the operation (exclusive or internal queue, a queue
    # in exclusive use, conflicting arguments)
    class Refused < Error; end

    # A shovel or federation upstream parameter that can't be used
    class ConfigError < Error; end

    # A message delivered to a consumer.
    #
    # `properties` and `body` are borrowed: for a local session they point
    # into the queue's memory mapped segment, kept mapped only until the
    # consume block returns. Anything that keeps them longer must copy them.
    record Delivery,
      tag : UInt64,
      exchange : String,
      routing_key : String,
      properties : AMQ::Protocol::Properties,
      body : Bytes,
      redelivered : Bool

    # A URI without host (and without user) is this broker
    def self.local?(uri : URI) : Bool
      uri.scheme.in?("amqp", "amqps") && uri.host.to_s.empty? && uri.user.nil?
    end

    # The vhost a URI names, `/` when the path is empty, like amqp-client
    def self.vhost_name(uri : URI) : String
      path = URI.decode(uri.path.lchop("/"))
      path.empty? ? "/" : path
    end

    # The `uri` field of a shovel or federation upstream parameter: a URI or,
    # as RabbitMQ accepts, a list of them
    def self.parse_uris(value : JSON::Any?) : Array(URI)
      return Array(URI).new if value.nil?
      strings = if uri = value.as_s?
                  [uri]
                elsif list = value.as_a?
                  list.map { |v| v.as_s? || raise ConfigError.new("Field 'uri' must be a string or a list of strings") }
                else
                  raise ConfigError.new("Field 'uri' must be a string or a list of strings")
                end
      strings.map { |str| URI.parse(str) }
    end

    # The URI for logs, status and x-received-from headers, without credentials
    def self.display_uri(uri : URI) : String
      return uri.to_s if uri.userinfo.nil?
      display = uri.dup
      display.user = nil
      display.password = nil
      display.to_s
    end

    abstract class Session
      # Shown as the connection name (remote) or consumer's connection (local)
      getter name : String
      # Bumped by every #open. Delivery tags are only meaningful within one
      # generation: a reopened session numbers its deliveries from 1 again.
      getter generation = 0_u32

      def initialize(@name : String)
      end

      # Connect. Raises if the endpoint can't be reached.
      abstract def open : Nil

      # Close the session. Unacked deliveries are returned to their queues.
      abstract def close : Nil

      abstract def closed? : Bool

      # Whether acks are worth batching: true when each is a round trip to
      # the broker
      abstract def batch_acks? : Bool

      # Declares a queue, or with `passive` checks that it exists (raising
      # `NotFound` if it doesn't). Returns its name, which the server generates
      # for an empty `name`, and message count.
      abstract def declare_queue(name : String, passive : Bool, durable = true,
                                 auto_delete = false,
                                 args = AMQ::Protocol::Table.new) : Tuple(String, UInt32)

      # Declares a durable exchange, or with `passive` checks that it exists
      abstract def declare_exchange(name : String, type : String, passive : Bool,
                                    args = AMQ::Protocol::Table.new) : Nil

      # Declares the queue unless it exists, keeping an existing one as it is
      def ensure_queue(name : String, durable = true, auto_delete = false,
                       args = AMQ::Protocol::Table.new) : Tuple(String, UInt32)
        declare_queue(name, passive: true)
      rescue NotFound
        declare_queue(name, passive: false, durable: durable, auto_delete: auto_delete, args: args)
      end

      # Declares the exchange unless it exists, keeping an existing one as it is
      def ensure_exchange(name : String, type : String, args = AMQ::Protocol::Table.new) : Nil
        declare_exchange(name, type, passive: true)
      rescue NotFound
        declare_exchange(name, type, passive: false, args: args)
      end

      abstract def delete_queue(name : String) : Nil
      abstract def delete_exchange(name : String) : Nil
      abstract def bind_queue(queue : String, exchange : String, routing_key : String,
                              args = AMQ::Protocol::Table.new) : Nil
      abstract def bind_exchange(destination : String, source : String, routing_key : String,
                                 args = AMQ::Protocol::Table.new) : Nil
      abstract def unbind_exchange(destination : String, source : String, routing_key : String,
                                   args = AMQ::Protocol::Table.new) : Nil

      # The most unacked deliveries consumers of this session may have
      abstract def prefetch=(count : UInt16)

      # Consumes `queue`, calling the block for each delivery, until the
      # consumer is cancelled (by #cancel, or the queue is deleted) or the
      # session closes. Blocks the calling fiber. Raises `ClosedError` (or
      # `NotFound` for a deleted queue) if the session closes underneath it.
      #
      # Deliveries are tagged 1, 2, 3, … per session. Unless `no_ack`, each
      # must be settled with #ack or #reject; that may happen from another
      # fiber, and after the consumer is cancelled.
      abstract def consume(queue : String, tag : String, no_ack : Bool, exclusive : Bool,
                           args : AMQ::Protocol::Table, &blk : Delivery -> Nil) : Nil

      abstract def cancel(tag : String) : Nil
      abstract def ack(delivery_tag : UInt64, multiple = false) : Nil
      abstract def reject(delivery_tag : UInt64, requeue : Bool) : Nil

      # Publish without confirm
      abstract def publish(exchange : String, routing_key : String,
                           properties : AMQ::Protocol::Properties, body : Bytes) : Nil

      # Publish and get called back with the broker's confirm (true) or nack
      # (false). Pending confirms are nacked when the session closes. The
      # callback may run on another fiber, or before publish returns.
      abstract def publish(exchange : String, routing_key : String,
                           properties : AMQ::Protocol::Properties, body : Bytes,
                           &on_confirm : Bool -> Nil) : Nil

      protected def next_generation : Nil
        @generation &+= 1
      end
    end
  end
end
