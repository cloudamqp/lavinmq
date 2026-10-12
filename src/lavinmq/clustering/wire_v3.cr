require "../clustering"

module LavinMQ
  module Clustering
    # Replication protocol version 3: the stream from the leader to a
    # follower, and the follower's acks.
    #
    # Every record names the journal entry it carries:
    #
    #   [u64 lsn][u8 op][i32 path_len][path]
    #     Append:  [i64 offset][i64 len][payload]   written at offset, idempotent
    #     Replace: [i64 len][payload]               whole file, via tmp + rename
    #     Delete:  nothing
    #     Fsync:   nothing                          "" means the whole data dir
    #
    # Lsns increase by one per record within a connection. An ack is
    #
    #   [i64 applied_lsn][i64 received_bytes]
    #
    # with both values absolute for the connection: the highest lsn whose
    # record is fully applied (and durable, when an Fsync record precedes
    # it), and the stream bytes received so far, reported per chunk inside a
    # large record so the leader sees progress. Both sides size records with
    # #record_size so the leader's sent count and the follower's received
    # count can't drift.
    module WireV3
      enum Op : UInt8
        Append
        Replace
        Delete
        Fsync
      end

      HEADER_SIZE = 13 # u64 lsn, u8 op, i32 path length
      ACK_SIZE    = 16 # i64 applied lsn, i64 received bytes

      def self.record_size(op : Op, path : String, payload_size : Int = 0) : Int64
        size = (HEADER_SIZE + path.bytesize).to_i64
        case op
        in .append?  then size + 2 * sizeof(Int64) + payload_size
        in .replace? then size + sizeof(Int64) + payload_size
        in .delete?, .fsync?
          size
        end
      end

      def self.write_header(io : IO, lsn : UInt64, op : Op, path : String) : Nil
        io.write_bytes lsn, IO::ByteFormat::LittleEndian
        io.write_byte op.value
        io.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
        io.write path.to_slice
      end

      # Returns the record's size on the wire
      def self.write_append(io : IO, lsn : UInt64, path : String, offset : Int64, bytes : Bytes) : Int64
        write_header(io, lsn, Op::Append, path)
        io.write_bytes offset, IO::ByteFormat::LittleEndian
        io.write_bytes bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
        io.write bytes
        record_size(Op::Append, path, bytes.bytesize)
      end

      def self.write_replace(io : IO, lsn : UInt64, path : String, bytes : Bytes) : Int64
        write_header(io, lsn, Op::Replace, path)
        io.write_bytes bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
        io.write bytes
        record_size(Op::Replace, path, bytes.bytesize)
      end

      def self.write_delete(io : IO, lsn : UInt64, path : String) : Int64
        write_header(io, lsn, Op::Delete, path)
        record_size(Op::Delete, path)
      end

      def self.write_fsync(io : IO, lsn : UInt64, path : String) : Int64
        write_header(io, lsn, Op::Fsync, path)
        record_size(Op::Fsync, path)
      end

      # After authenticating, a follower tells the leader where it is: the id
      # of the journal it last streamed and the lsn it has durably applied (0
      # and 0 for none). The leader answers whether it resumes the stream
      # from there (1) or starts a full sync (0), and with its journal id.
      # After a full sync's second pass the leader sends the lsn the stream
      # starts at, so the follower can record its position.
      RESUME_FULL_SYNC = 0u8
      RESUME_STREAM    = 1u8

      def self.write_resume_request(io : IO, journal_id : UInt64, applied_lsn : UInt64) : Nil
        buf = uninitialized UInt8[16]
        slice = buf.to_slice
        IO::ByteFormat::LittleEndian.encode(journal_id, slice[0, 8])
        IO::ByteFormat::LittleEndian.encode(applied_lsn, slice[8, 8])
        io.write slice
      end

      def self.read_resume_request(io : IO) : {UInt64, UInt64}
        buf = uninitialized UInt8[16]
        io.read_fully(buf.to_slice)
        {IO::ByteFormat::LittleEndian.decode(UInt64, buf.to_slice[0, 8]),
         IO::ByteFormat::LittleEndian.decode(UInt64, buf.to_slice[8, 8])}
      end

      def self.write_resume_reply(io : IO, resume : Bool, journal_id : UInt64) : Nil
        buf = uninitialized UInt8[9]
        slice = buf.to_slice
        slice[0] = resume ? RESUME_STREAM : RESUME_FULL_SYNC
        IO::ByteFormat::LittleEndian.encode(journal_id, slice[1, 8])
        io.write slice
      end

      def self.read_resume_reply(io : IO) : {Bool, UInt64}
        buf = uninitialized UInt8[9]
        io.read_fully(buf.to_slice)
        resume = case buf[0]
                 when RESUME_STREAM    then true
                 when RESUME_FULL_SYNC then false
                 else                       raise ProtocolError.new("Unknown resume reply #{buf[0]}")
                 end
        {resume, IO::ByteFormat::LittleEndian.decode(UInt64, buf.to_slice[1, 8])}
      end

      def self.write_cut(io : IO, lsn : UInt64) : Nil
        io.write_bytes lsn, IO::ByteFormat::LittleEndian
      end

      def self.read_cut(io : IO) : UInt64
        io.read_bytes UInt64, IO::ByteFormat::LittleEndian
      end

      record Header, lsn : UInt64, op : Op, path : String

      def self.read_header(io : IO) : Header
        lsn = io.read_bytes UInt64, IO::ByteFormat::LittleEndian
        op = Op.from_value?(io.read_byte || raise IO::EOFError.new) ||
             raise ProtocolError.new("Unknown replication record type")
        path_len = io.read_bytes Int32, IO::ByteFormat::LittleEndian
        raise ProtocolError.new("Invalid path length #{path_len}") if path_len < 0
        Header.new(lsn, op, io.read_string(path_len))
      end

      # One write, so a reader can't see half an ack
      def self.write_ack(io : IO, applied_lsn : UInt64, received_bytes : Int64) : Nil
        buf = uninitialized UInt8[ACK_SIZE]
        slice = buf.to_slice
        IO::ByteFormat::LittleEndian.encode(applied_lsn.to_i64, slice[0, 8])
        IO::ByteFormat::LittleEndian.encode(received_bytes, slice[8, 8])
        io.write slice
      end

      def self.decode_ack(slice : Bytes) : {UInt64, Int64}
        {IO::ByteFormat::LittleEndian.decode(Int64, slice[0, 8]).to_u64!,
         IO::ByteFormat::LittleEndian.decode(Int64, slice[8, 8])}
      end
    end
  end
end
