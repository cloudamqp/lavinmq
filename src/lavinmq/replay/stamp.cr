require "amq-protocol"
require "uuid"
require "../rough_time"
require "../message"

module LavinMQ
  module Replay
    # Stamping helpers for the `x-queue-type: replay` queue type. The
    # constants intentionally mirror the cross-feature `x-source-*`
    # schema so a replay queue can be fed by:
    #
    # * an operator manually moving messages (Feature 1's `move_to`),
    # * DLX routing (here we derive `x-source-*` from `x-first-death-*`
    #   when a replay queue happens to be bound as a DLX destination),
    # * the replay queue's own `release` endpoint sending messages back.
    #
    # `x-replay-id` is the addressable handle used by the management API
    # to identify a specific message inside a replay queue. Every
    # message that lands in a replay queue gets one stamped if absent.
    HEADER_SOURCE_QUEUE       = "x-source-queue"
    HEADER_SOURCE_EXCHANGE    = "x-source-exchange"
    HEADER_SOURCE_ROUTING_KEY = "x-source-routing-key"
    HEADER_SOURCE_TIMESTAMP   = "x-source-timestamp"
    HEADER_SOURCE_RULE_ID     = "x-source-rule-id"
    HEADER_REPLAY_ID          = "x-replay-id"

    # x-death entries are stored as a table per redelivery. The "queue",
    # "exchange" and "routing-keys" fields cover what we need to
    # republish on release.
    FIRST_DEATH_QUEUE       = "x-first-death-queue"
    FIRST_DEATH_EXCHANGE    = "x-first-death-exchange"
    FIRST_DEATH_ROUTING_KEY = "x-first-death-routing-key"

    # Returns a new Properties with replay metadata applied to the
    # incoming Message:
    #
    # * `x-replay-id` is stamped if missing (UUID).
    # * If `x-source-queue` is missing but `x-first-death-queue` is
    #   present, the `x-source-*` triplet is derived from the death
    #   metadata. `x-first-death-exchange` falls back to the message's
    #   current exchange; `x-first-death-routing-key` (which LavinMQ's
    #   DLX layer does not stamp today) falls back to the message's
    #   current routing-key so a DLX-routed message can still be
    #   replayed even if the original routing-key is unavailable.
    #   Existing `x-source-*` headers are never overwritten.
    # * `x-source-timestamp` is stamped if missing.
    #
    # Raises `LavinMQ::Error::PreconditionFailed` when neither
    # `x-source-queue` nor `x-first-death-queue` is available so the
    # caller can refuse the push instead of silently storing an
    # unreplayable message.
    def self.stamp_intake(msg : ::LavinMQ::Message) : AMQ::Protocol::Properties
      properties = msg.properties
      headers = properties.headers.try(&.clone) || AMQ::Protocol::Table.new
      headers[HEADER_REPLAY_ID] ||= UUID.random.to_s
      unless headers.has_key?(HEADER_SOURCE_QUEUE)
        if death_queue = headers[FIRST_DEATH_QUEUE]?
          headers[HEADER_SOURCE_QUEUE] = death_queue
          headers[HEADER_SOURCE_EXCHANGE] = headers[FIRST_DEATH_EXCHANGE]? || msg.exchange_name
          headers[HEADER_SOURCE_ROUTING_KEY] = headers[FIRST_DEATH_ROUTING_KEY]? || msg.routing_key
        else
          raise LavinMQ::Error::PreconditionFailed.new(
            "Message lacks x-source-queue and x-first-death-queue; cannot enter replay queue")
        end
      end
      headers[HEADER_SOURCE_TIMESTAMP] ||= RoughTime.unix_ms
      AMQ::Protocol::Properties.new(
        content_type: properties.content_type,
        content_encoding: properties.content_encoding,
        headers: headers,
        delivery_mode: properties.delivery_mode,
        priority: properties.priority,
        correlation_id: properties.correlation_id,
        reply_to: properties.reply_to,
        expiration: properties.expiration,
        message_id: properties.message_id,
        timestamp: properties.timestamp_raw,
        type: properties.type,
        user_id: properties.user_id,
        app_id: properties.app_id,
        reserved1: properties.reserved1,
      )
    end
  end
end
