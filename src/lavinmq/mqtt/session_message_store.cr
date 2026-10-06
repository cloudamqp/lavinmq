require "../message_store"

module LavinMQ
  module MQTT
    # A session's message store, which also holds the packet ids of the messages
    # the session owes a resuming client. The ids live here so that a message
    # leaving the store takes its id with it, whichever path removed it.
    class SessionMessageStore < LavinMQ::MessageStore
      # Not guarded by the session's `@msg_store_lock`, unlike the rest of this
      # object: every access is a single `Hash` operation, which cannot yield.
      # Same shape as `Session#@inflight`, and the same caveat under
      # multi-threading, tracked in #2067.
      @original_packet_ids = Hash(SegmentPosition, UInt16).new

      def remember_original_packet_id(sp : SegmentPosition, packet_id : UInt16) : Nil
        @original_packet_ids[sp] = packet_id
      end

      def original_packet_id?(sp : SegmentPosition) : UInt16?
        @original_packet_ids[sp]? unless @original_packet_ids.empty?
      end

      def forget_original_packet_id(sp : SegmentPosition) : Nil
        @original_packet_ids.delete(sp) unless @original_packet_ids.empty?
      end

      # Linear, but bounded by the in-flight window that produced the ids.
      def original_packet_id_in_use?(id : UInt16) : Bool
        !@original_packet_ids.empty? && @original_packet_ids.has_value?(id)
      end

      # Called before the delete, while the message is still readable, for a
      # message that still owed a re-send under its original id. Fires for
      # drops (overflow, purge) but also for an ack racing the re-send, so the
      # handler must tell them apart. Returns whether the id is now released.
      property on_original_packet_id_dropped : Proc(SegmentPosition, UInt16, Bool)? = nil

      # Called for a released id once its delete is written
      property on_original_packet_id_released : Proc(UInt16, Nil)? = nil

      def delete(sp) : Nil
        released = nil
        unless @original_packet_ids.empty?
          if id = @original_packet_ids.delete(sp)
            released = id if @on_original_packet_id_dropped.try &.call(sp, id)
          end
        end
        super
        if id = released
          @on_original_packet_id_released.try &.call(id)
        end
      end
    end
  end
end
