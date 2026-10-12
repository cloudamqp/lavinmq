module LavinMQ::Clustering::Raft
  # The broker's changes to its data dir, as a batch in an entry's `data`.
  # Only descriptors: the bytes appended to files travel next to the entry as
  # its payload, the Append ops' bytes in op order, and are written straight
  # to the files, never to the Raft log.
  #
  # A batch is a header, `u8 version, u8 0, u16 0, u32 op count`, and its
  # ops, each a `u8` type and its fields, little endian. Files are known by
  # ids, one per inode, paths only appear where a file gets one.
  module Ops
    VERSION = 1u8
    HEADER  =   8
    Format  = IO::ByteFormat::LittleEndian

    enum Type : UInt8
      # A new file, at `path`, or with STAGING in `.raft_staging/<id>` until
      # installed. With DIR a directory.
      Create = 1
      # Bytes from the payload at `pos`
      Append = 2
      # Bytes at `pos`, in the op itself: acks and other small writes
      AppendInline = 3
      # A staged file takes `path`, what was there goes to the trash
      Install = 4
      # A file goes to the trash
      Delete = 5
      # A directory goes to the trash, with everything in it
      DeleteDir = 6
      # A file is cut to `size`, the rest goes to the trash
      TruncateTo = 7
      # A file that was already there, as when a cluster starts on a data dir
      Adopt = 8
    end

    STAGING = 1u8
    DIR     = 2u8

    # One op, as decoded. `path` and `bytes` are only set for the ops that
    # have them, `bytes` points into the batch.
    record Op, type : Type, id : UInt32 = 0u32, pos : Int64 = 0i64, len : Int64 = 0i64,
      tid : Int64 = 0i64, flags : UInt8 = 0u8, path : String? = nil, bytes : Bytes? = nil do
      def path! : String
        path || raise CorruptError.new("#{type} without a path")
      end

      def bytes! : Bytes
        bytes || raise CorruptError.new("#{type} without bytes")
      end
    end

    class CorruptError < Exception; end

    # Builds a batch. Appends to a file right after its previous append in the
    # same batch extend that op, so a stream of messages to a segment is one
    # op. Reused: #clear starts the next batch.
    class Batch
      getter op_count = 0
      # Bytes the Append ops take from the payload
      getter payload_bytes = 0i64
      @io = IO::Memory.new
      # File id → where its last Append op is in @io, and where it ends
      @last_append = Hash(UInt32, Tuple(Int32, Int64)).new

      def initialize
        clear
      end

      def clear : Nil
        @io.clear
        @io.write_byte VERSION
        @io.write_byte 0u8
        @io.write_bytes 0u16, Format
        @io.write_bytes 0u32, Format
        @op_count = 0
        @payload_bytes = 0i64
        @last_append.clear
      end

      def empty? : Bool
        @op_count.zero?
      end

      # Bytes of the batch so far
      def bytesize : Int32
        @io.size
      end

      def create(id : UInt32, path : String, flags : UInt8 = 0u8) : Nil
        op Type::Create, id
        @io.write_byte flags
        write_path path
      end

      def append(id : UInt32, pos : Int64, len : Int64) : Nil
        @payload_bytes += len
        if (last = @last_append[id]?) && last[1] == pos
          offset = last[0]
          len_at = offset + 1 + 4 + 8
          current = Format.decode(Int64, @io.to_slice[len_at, 8])
          Format.encode(current + len, @io.to_slice[len_at, 8])
          @last_append[id] = {offset, pos + len}
          return
        end
        offset = @io.size
        op Type::Append, id
        @io.write_bytes pos, Format
        @io.write_bytes len, Format
        @last_append[id] = {offset, pos + len}
      end

      def append_inline(id : UInt32, pos : Int64, bytes : Bytes) : Nil
        op Type::AppendInline, id
        @io.write_bytes pos, Format
        @io.write_bytes bytes.size, Format
        @io.write bytes
        @last_append.delete(id)
      end

      def install(id : UInt32, tid : Int64, path : String) : Nil
        op Type::Install, id
        @io.write_bytes tid, Format
        write_path path
        @last_append.delete(id)
      end

      def delete(id : UInt32, tid : Int64) : Nil
        op Type::Delete, id
        @io.write_bytes tid, Format
        @last_append.delete(id)
      end

      def delete_dir(tid : Int64, path : String) : Nil
        op Type::DeleteDir, 0u32
        @io.write_bytes tid, Format
        write_path path
        @last_append.clear
      end

      def truncate_to(id : UInt32, size : Int64, tid : Int64) : Nil
        op Type::TruncateTo, id
        @io.write_bytes size, Format
        @io.write_bytes tid, Format
        @last_append.delete(id)
      end

      def adopt(id : UInt32, size : Int64, path : String) : Nil
        op Type::Adopt, id
        @io.write_bytes size, Format
        write_path path
      end

      # The batch, valid until the next change
      def to_slice : Bytes
        Format.encode(@op_count.to_u32, @io.to_slice[4, 4])
        @io.to_slice
      end

      private def op(type : Type, id : UInt32) : Nil
        @io.write_byte type.value
        @io.write_bytes id, Format
        @op_count += 1
      end

      private def write_path(path : String) : Nil
        raise ArgumentError.new("Path too long: #{path}") if path.bytesize > UInt16::MAX
        @io.write_bytes path.bytesize.to_u16, Format
        @io.write path.to_slice
      end
    end

    # Yields each op of *data*
    def self.each(data : Bytes, & : Op ->) : Nil
      raise CorruptError.new("Op batch too short") if data.size < HEADER
      raise CorruptError.new("Unknown op batch version #{data[0]}") unless data[0] == VERSION
      count = Format.decode(UInt32, data[4, 4])
      pos = HEADER
      count.times do
        op, pos = read_op(data, pos)
        yield op
      end
      raise CorruptError.new("Op batch has #{data.size - pos} trailing bytes") unless pos == data.size
    rescue ex : IndexError
      raise CorruptError.new("Op batch cut short", cause: ex)
    end

    # The bytes the batch's Append ops take from the payload
    def self.payload_bytes(data : Bytes) : Int64
      total = 0i64
      each(data) { |op| total += op.len if op.type.append? }
      total
    end

    private def self.read_op(data : Bytes, pos : Int32) : Tuple(Op, Int32)
      type = Type.from_value?(data[pos]) || raise CorruptError.new("Unknown op type #{data[pos]}")
      id = Format.decode(UInt32, data[pos + 1, 4])
      pos += 5
      case type
      in .create?
        flags = data[pos]
        path, pos = read_path(data, pos + 1)
        {Op.new(type, id, flags: flags, path: path), pos}
      in .append?
        {Op.new(type, id, Format.decode(Int64, data[pos, 8]), Format.decode(Int64, data[pos + 8, 8])), pos + 16}
      in .append_inline?
        at = Format.decode(Int64, data[pos, 8])
        len = Format.decode(Int32, data[pos + 8, 4])
        raise CorruptError.new("Negative inline length") if len < 0
        {Op.new(type, id, at, len.to_i64, bytes: data[pos + 12, len]), pos + 12 + len}
      in .install?
        tid = Format.decode(Int64, data[pos, 8])
        path, pos = read_path(data, pos + 8)
        {Op.new(type, id, tid: tid, path: path), pos}
      in .delete?
        {Op.new(type, id, tid: Format.decode(Int64, data[pos, 8])), pos + 8}
      in .delete_dir?
        tid = Format.decode(Int64, data[pos, 8])
        path, pos = read_path(data, pos + 8)
        {Op.new(type, id, tid: tid, path: path), pos}
      in .truncate_to?
        size = Format.decode(Int64, data[pos, 8])
        {Op.new(type, id, size, tid: Format.decode(Int64, data[pos + 8, 8])), pos + 16}
      in .adopt?
        size = Format.decode(Int64, data[pos, 8])
        path, pos = read_path(data, pos + 8)
        {Op.new(type, id, size, path: path), pos}
      end
    end

    private def self.read_path(data : Bytes, pos : Int32) : Tuple(String, Int32)
      len = Format.decode(UInt16, data[pos, 2]).to_i
      {String.new(data[pos + 2, len]), pos + 2 + len}
    end
  end
end
