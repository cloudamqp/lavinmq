require "../message_store"

module LavinMQ
  module MQTT
    # A session's message store, which also holds the packet ids of the messages
    # the session owes a resuming client. The ids live here so that a message
    # leaving the store takes its id with it, whichever path removed it.
    class SessionMessageStore < LavinMQ::MessageStore
      # Not guarded by the session's `@msg_store_lock`, unlike the rest of this
      # object: every access is a single `Hash` operation, which cannot yield.
      # Same shape as `Session#@unacked`, and the same caveat under
      # multi-threading, tracked in #2067.
      @packet_ids = Hash(SegmentPosition, UInt16).new

      def remember_packet_id(sp : SegmentPosition, packet_id : UInt16) : Nil
        @packet_ids[sp] = packet_id
      end

      def packet_id?(sp : SegmentPosition) : UInt16?
        @packet_ids[sp]? unless @packet_ids.empty?
      end

      def forget_packet_id(sp : SegmentPosition) : Nil
        @packet_ids.delete(sp) unless @packet_ids.empty?
      end

      def delete(sp) : Nil
        super
        forget_packet_id(sp)
      end
    end
  end
end
