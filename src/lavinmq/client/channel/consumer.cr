require "log"
require "../../sortable_json"
require "../../error"
require "../../queue/queue"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON

        alias ConsumerDetails = NamedTuple(
          queue: {name: String, vhost: String},
          consumer_tag: String,
          exclusive: Bool,
          ack_required: Bool,
          prefetch_count: UInt16,
          priority: Int32,
          channel_details: {peer_host: String?, peer_port: Int32?, connection_name: String, user: String, number: UInt16, name: String},
        )

        abstract def tag : String
        abstract def priority : Int32
        abstract def exclusive? : Bool
        abstract def no_ack? : Bool
        abstract def channel : Channel
        abstract def queue : Queue

        abstract def prefetch_count : UInt16
        abstract def unacked : UInt32
        abstract def closed? : Bool

        abstract def prefetch_count=(prefetch_count : UInt16)
        abstract def accepts? : Bool
        abstract def has_capacity : ::Channel(Bool)
        abstract def details_tuple : ConsumerDetails

        class ClosedError < Error; end
      end
    end
  end
end
