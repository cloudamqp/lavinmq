require "./source"
require "../endpoint/session"
require "./postgres/connection"
require "./postgres/pgoutput"
require "digest/sha1"

module LavinMQ
  module Shovel
    # Streams committed transactions from a PostgreSQL logical replication
    # slot (pgoutput plugin, publication based) and yields one JSON message per
    # transaction, see Postgres::PgOutput.
    #
    # Delivery is at-least-once. The slot's confirmed_flush_lsn only advances
    # past a transaction once it, and every transaction before it, has been
    # acked (the settlement frontier, as in AMQPSource). Anything not yet
    # acked is streamed again by the server after a reconnect. A requeue
    # can't put back a single transaction, so it reconnects instead.
    class PostgresSource < Source
      Log             = LavinMQ::Log.for "shovel.postgres_source"
      STATUS_INTERVAL = 10.seconds

      getter delete_after = DeleteAfter::Never
      # How far the slot is confirmed, as last reported to the server
      getter flushed_lsn = 0_u64

      @conn : Postgres::Connection?
      # Serializes the frontier and every write to the connection; the
      # consuming fiber, the confirm fiber and the status loop all write.
      @lock = Mutex.new
      # Delivered transactions in stream order, {delivery tag, end LSN}, up to
      # the first unsettled one; settled tags past the gap wait in @settled.
      @pending = Deque({UInt64, UInt64}).new
      @settled = Set(UInt64).new
      @received_lsn = 0_u64
      @last_tag = 0_u64
      @restart_reason : String?
      # Signalled when a settlement frees room in the prefetch window
      @space = ::Channel(Nil).new(1)
      # Bumped by every stop, so a start that a stop overtook gives up
      @stops = 0_u64
      # The connection a start is setting up, for a stop to close
      @starting : Postgres::Connection?

      def initialize(@name : String, @uri : URI, @publication : String, @slot : String,
                     @prefetch = DEFAULT_PREFETCH,
                     @ack_mode = DEFAULT_ACK_MODE)
      end

      # Shovel names are only unique within a vhost, and slot names are
      # sanitized and capped at 63 characters, so the readable prefix is
      # followed by a hash of the vhost and the exact name.
      def self.default_slot_name(vhost : String, shovel_name : String) : String
        prefix = shovel_name.downcase.gsub(/[^a-z0-9_]/, '_')[0, 46]
        hash = Digest::SHA1.hexdigest("#{vhost}/#{shovel_name}")[0, 8]
        "lavinmq_#{prefix}_#{hash}"
      end

      # Setting up can block for long: CREATE_REPLICATION_SLOT waits for every
      # running transaction to end. A stop (pause, delete) meanwhile closes the
      # connection being set up, and the start raises instead of leaving a
      # stopped shovel streaming, its slot held active.
      def start
        return if started?
        stops = @stops
        conn = Postgres::Connection.connect(@uri, "LavinMQ shovel #{@name}")
        begin
          @lock.synchronize do
            raise_if_stopped(stops)
            @starting = conn
          end
          slot_lsn = ensure_slot(conn)
          conn.start_replication(@slot, @publication)
          @lock.synchronize do
            raise_if_stopped(stops)
            @starting = nil
            # The server streams from the slot's position, so that's where both
            # what's received and what's flushed start.
            @flushed_lsn = @received_lsn = slot_lsn
            @pending.clear
            @settled.clear
            @restart_reason = nil
            @conn = conn
          end
        rescue ex
          @lock.synchronize { @starting = nil if @starting.same?(conn) }
          conn.close
          raise ex
        end
        Log.info { "name=#{@name} streaming slot=#{@slot} publication=#{@publication}" }
        spawn(name: "Shovel #{@name} postgres status") { status_loop(conn) }
      end

      private def raise_if_stopped(stops)
        raise Postgres::Error.new("Stopped while starting") unless @stops == stops
      end

      # Creates the slot unless it exists, and returns its confirmed LSN.
      private def ensure_slot(conn) : UInt64
        rows = conn.query("SELECT confirmed_flush_lsn FROM pg_replication_slots " \
                          "WHERE slot_name = #{Postgres.quote_literal(@slot)}")
        if row = rows.first?
          return row[0].try { |lsn| Postgres::LSN.parse(lsn) } || 0_u64
        end
        # Columns: slot_name, consistent_point, snapshot_name, output_plugin
        created = conn.query("CREATE_REPLICATION_SLOT #{Postgres.quote_ident(@slot)} LOGICAL pgoutput NOEXPORT_SNAPSHOT")
        Log.info { "name=#{@name} created replication slot #{@slot}" }
        created.dig?(0, 1).try { |lsn| Postgres::LSN.parse(lsn) } || 0_u64
      end

      def stop
        conn = @lock.synchronize do
          @stops &+= 1
          @starting.try &.close
          @starting = nil
          c = @conn
          @conn = nil
          if c && !c.closed?
            begin
              c.send_standby_status(@received_lsn, @flushed_lsn, @flushed_lsn)
              c.send_copy_done
            rescue IO::Error | OpenSSL::Error
            end
          end
          c
        end
        conn.try &.close
        wake
      end

      def started? : Bool
        (c = @conn) ? !c.closed? : false
      end

      def each(&blk : Endpoint::Delivery -> Nil)
        conn = @conn || raise "Not started"
        decoder = Postgres::PgOutput.new
        loop do
          wait_for_room(conn)
          data = conn.read_copy_data
          break if data.nil?
          case data[0].unsafe_chr
          when 'w' # XLogData
            tx = decoder.decode(data + 25) || next
            if tag = register(tx)
              blk.call message(tx, tag)
              ack(tag) if @ack_mode.no_ack?
            end
          when 'k' # Primary keepalive
            wal_end = IO::ByteFormat::BigEndian.decode(UInt64, data[1, 8])
            keepalive(conn, wal_end, data[17] == 1, decoder.in_transaction?)
          end
        end
        raise Postgres::Error.new("Replication stream ended by server") if @conn.same?(conn)
      rescue ex
        # Stopped by the Runner (pause, terminate): the closed connection is
        # expected, return quietly.
        return unless @conn.same?(conn)
        reason = @restart_reason
        stop
        raise Postgres::Error.new(reason) if reason
        Log.warn { "name=#{@name} #{ex.message}" }
        raise ex
      end

      # Assigns a delivery tag to a transaction. One without changes in the
      # publication (PostgreSQL 14 sends every commit, 15+ skips them) gets none and takes no
      # room in the prefetch window: its LSN is flushed right away, or, with
      # transactions in flight, once the one before it settles.
      private def register(tx : Postgres::PgOutput::Transaction) : UInt64?
        @lock.synchronize do
          @received_lsn = Math.max(@received_lsn, tx.end_lsn)
          if tx.empty?
            if last = @pending.last?
              @pending[-1] = {last[0], Math.max(last[1], tx.end_lsn)}
            else
              @flushed_lsn = Math.max(@flushed_lsn, tx.end_lsn)
            end
            return
          end
          tag = @last_tag += 1
          @pending << {tag, tx.end_lsn}
          tag
        end
      end

      private def message(tx, tag) : Endpoint::Delivery
        lsn = Postgres::LSN.format(tx.commit_lsn)
        headers = AMQ::Protocol::Table.new
        headers["x-pg-lsn"] = lsn
        headers["x-pg-xid"] = tx.xid.to_i64
        props = AMQ::Protocol::Properties.new(
          content_type: "application/json",
          delivery_mode: 2_u8,
          message_id: lsn,
          timestamp: tx.commit_time,
          headers: headers)
        # The body is owned by the transaction, so it outlives the block too
        Endpoint::Delivery.new(tag, "", @publication, props, tx.body, false)
      end

      def ack(delivery_tag)
        @lock.synchronize { settle(delivery_tag) }
      end

      # A transaction can't be returned to the server on its own: with requeue
      # the connection is closed, and the server streams it again, with every
      # other unacked one, after the Runner reconnects. Without requeue it is
      # dropped, i.e. settled like an ack.
      def reject(delivery_tag, requeue)
        unless requeue
          Log.warn { "name=#{@name} dropping rejected transaction (delivery tag #{delivery_tag})" }
          return ack(delivery_tag)
        end
        @lock.synchronize do
          conn = @conn || return
          return if @restart_reason
          @restart_reason = "Delivery failed, streaming again from #{Postgres::LSN.format(@flushed_lsn)}"
          conn.close
        end
        wake
      end

      # Moves the frontier over `tag`, and any settled tags behind it, and
      # confirms the new position to the server. Must hold @lock.
      private def settle(tag : UInt64)
        conn = @conn || return
        first = @pending.first? || return
        return if tag < first[0]
        @settled << tag
        advanced = false
        while (head = @pending.first?) && @settled.delete(head[0])
          @pending.shift
          @flushed_lsn = head[1]
          advanced = true
        end
        return unless advanced
        send_status(conn)
        wake
      end

      # With nothing in flight the stream is fully handled up to the server's
      # WAL end, so confirm that too; otherwise an idle publication would hold
      # back WAL that only other tables wrote.
      private def keepalive(conn, wal_end : UInt64, reply : Bool, in_transaction : Bool)
        @lock.synchronize do
          return unless @conn.same?(conn)
          @received_lsn = Math.max(@received_lsn, wal_end)
          if @pending.empty? && !in_transaction && wal_end > @flushed_lsn
            @flushed_lsn = wal_end
          end
          send_status(conn) if reply
        end
      end

      private def send_status(conn)
        return if conn.closed?
        conn.send_standby_status(@received_lsn, @flushed_lsn, @flushed_lsn)
      rescue ex : IO::Error | OpenSSL::Error
        Log.debug { "name=#{@name} could not send status: #{ex.message}" }
      end

      # The server disconnects a client that is silent for wal_sender_timeout,
      # also while the consumer waits for room in the prefetch window.
      private def status_loop(conn)
        loop do
          sleep STATUS_INTERVAL
          @lock.synchronize do
            return unless @conn.same?(conn)
            send_status(conn)
          end
        end
      end

      private def wait_for_room(conn)
        # A prefetch of 0 is unlimited, as for AMQP
        while @prefetch > 0 && @pending.size >= @prefetch
          return if conn.closed? || !@conn.same?(conn)
          @space.receive
        end
      end

      private def wake
        select
        when @space.send(nil)
        else
        end
      end
    end
  end
end
