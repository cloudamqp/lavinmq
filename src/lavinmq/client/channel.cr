require "./channel/consumer"
require "./client"
require "../queue"
require "../stats"
require "../sortable_json"
require "../error"

module LavinMQ
  abstract class Client
    abstract class Channel
      include Stats
      include SortableJSON

      rate_stats({"ack", "get", "publish", "deliver", "redeliver", "reject", "confirm", "return_unroutable"})
      alias ChannelDetails = NamedTuple(
        number: UInt16,
        name: String,
        vhost: String,
        user: String,
        consumer_count: Int32,
        prefetch_count: UInt16,
        global_prefetch_count: UInt16,
        confirm: Bool,
        transactional: Bool,
        messages_unacknowledged: Int32,
        connection_details: ConnectionDetails,
        state: String,
        message_stats: StatsDetails)

      record Unack,
        tag : UInt64,
        queue : Queue,
        sp : SegmentPosition,
        consumer : Consumer?,
        delivered_at : Time::Span

      abstract def id : UInt16
      abstract def name : String
      abstract def running? : Bool
      abstract def flow? : Bool
      abstract def log : Log
      abstract def consumers : Array(Consumer)
      abstract def prefetch_count : UInt16
      abstract def global_prefetch_count : UInt16
      abstract def has_capacity : ::Channel(Nil)
      abstract def check_consumer_timeout
      abstract def state : String
      abstract def details_tuple : ChannelDetails

      class ClosedError < Error; end
    end
  end
end
