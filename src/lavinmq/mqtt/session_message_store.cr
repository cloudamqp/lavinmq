require "../message_store"

module LavinMQ
  module MQTT
    # A session's message store, which also holds the packet ids of the messages
    # the session owes a resuming client. The ids live here so that a message
    # leaving the store takes its id with it, whichever path removed it.
    class SessionMessageStore < LavinMQ::MessageStore
      # Packet ids by message, with a count per id so that `next_packet_id`,
      # which tests every candidate, does not scan them all. One id can belong
      # to two messages only through a bug, but the count keeps it in use until
      # both are gone.
      class OriginalPacketIds
        @ids = Hash(SegmentPosition, UInt16).new
        @counts = Hash(UInt16, UInt32).new

        def []=(sp : SegmentPosition, id : UInt16) : UInt16
          delete(sp)
          @ids[sp] = id
          @counts[id] = (@counts[id]? || 0u32) + 1
          id
        end

        def []?(sp : SegmentPosition) : UInt16?
          @ids[sp]?
        end

        def delete(sp : SegmentPosition) : UInt16?
          id = @ids.delete(sp) || return
          count = @counts[id] - 1
          count.zero? ? @counts.delete(id) : (@counts[id] = count)
          id
        end

        def in_use?(id : UInt16) : Bool
          @counts.has_key?(id)
        end

        delegate empty?, size, keys, key_for, to: @ids
      end

      # Not guarded by the session's `@msg_store_lock`, unlike the rest of this
      # object: every access is a few `Hash` operations, which cannot yield.
      # Same shape as `Session#@inflight`, and the same caveat under
      # multi-threading, tracked in #2067.
      @original_packet_ids = OriginalPacketIds.new

      def remember_original_packet_id(sp : SegmentPosition, packet_id : UInt16) : Nil
        @original_packet_ids[sp] = packet_id
      end

      def original_packet_id?(sp : SegmentPosition) : UInt16?
        @original_packet_ids[sp]? unless @original_packet_ids.empty?
      end

      def forget_original_packet_id(sp : SegmentPosition) : Nil
        @original_packet_ids.delete(sp) unless @original_packet_ids.empty?
      end

      def original_packet_id_in_use?(id : UInt16) : Bool
        @original_packet_ids.in_use?(id)
      end

      # Called before the delete, while the message is still readable, for a
      # message that still owed a re-send under its original id. Fires for
      # drops (overflow, purge) but also for an ack racing the re-send, so the
      # handler must tell them apart. Returns whether the id is now released.
      property on_original_packet_id_dropped : Proc(SegmentPosition, UInt16, Bool)? = nil

      # Called for a released id once its delete is written and marked dirty
      property on_original_packet_id_released : Proc(UInt16, Nil)? = nil

      # Like `shift_with_lease?`, for a caller that holds several messages at
      # once: call under the store's lock, and `Envelope#release` each one.
      def shift_leased? : Envelope?
        shift?.try &.lease
      end

      # Whether `sp` is still a message in this store. Positions are never
      # reused, so a missing one is a deleted one. Only exact at load: a
      # delete after that is not reflected in `@deleted`.
      def includes?(sp : SegmentPosition) : Bool
        @segments.has_key?(sp.segment) && !deleted?(sp.segment, sp.position)
      end

      # Makes the delete of `sp` part of the next persister drain. When the
      # delete emptied the segment, its files were unlinked instead, which
      # only the directory's sync makes durable.
      def mark_delete_dirty(sp : SegmentPosition) : Nil
        persister = @persister || return
        if afile = @acks[sp.segment]?
          persister.mark_dirty(afile)
        else
          persister.mark_dirty(@msg_dir)
        end
      end

      def delete(sp) : Nil
        released = nil
        unless @original_packet_ids.empty?
          if id = @original_packet_ids.delete(sp)
            released = id if @on_original_packet_id_dropped.try &.call(sp, id)
          end
        end
        super
        if id = released
          mark_delete_dirty(sp)
          @on_original_packet_id_released.try &.call(id)
        end
      end
    end
  end
end
