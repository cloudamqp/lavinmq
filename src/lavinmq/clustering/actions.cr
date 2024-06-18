require "../mfile"

module LavinMQ
  module Clustering
    record FileRange, mfile : MFile, pos : Int32, len : Int32 do
      def to_slice : Bytes
        mfile.to_slice(pos, len)
      end
    end

    abstract struct Action
      def initialize(@data_dir : String, @filename : String)
      end

      abstract def lag_size : Int64
      abstract def send(socket : IO) : Int64

      getter filename

      private def send_filename(socket : IO)
        socket.write_bytes filename.bytesize.to_i32, IO::ByteFormat::LittleEndian
        socket.write filename.to_slice
      end
    end

    struct AddAction < Action
      def initialize(@data_dir : String, @filename : String, @mfile : MFile? = nil)
      end

      def lag_size : Int64
        if mfile = @mfile
          0i64 + sizeof(Int32) + filename.bytesize +
            sizeof(Int64) + mfile.size.to_i64
        else
          0i64 + sizeof(Int32) + filename.bytesize +
            sizeof(Int64) + File.size(File.join(@data_dir, filename)).to_i64
        end
      end

      def send(socket) : Int64
        Log.debug { "Add #{@filename}" }
        send_filename(socket)
        if mfile = @mfile
          size = mfile.size.to_i64
          socket.write_bytes size, IO::ByteFormat::LittleEndian
          mfile.copy_to(socket, size)
          size
        else
          File.open(File.join(@data_dir, @filename)) do |f|
            size = f.size.to_i64
            socket.write_bytes size, IO::ByteFormat::LittleEndian
            IO.copy(f, socket, size) == size || raise IO::EOFError.new
            size
          end
        end
      end
    end

    struct AppendAction < Action
      def initialize(@data_dir : String, @filename : String, @obj : Bytes | FileRange | UInt32 | Int32)
      end

      def lag_size : Int64
        datasize = case obj = @obj
                   in Bytes
                     obj.bytesize.to_i64
                   in FileRange
                     obj.len.to_i64
                   in UInt32, Int32
                     4i64
                   end
        0i64 + sizeof(Int32) + filename.bytesize +
          sizeof(Int64) + datasize
      end

      def send(socket) : Int64
        send_filename(socket)
        len : Int64
        case obj = @obj
        in Bytes
          len = obj.bytesize.to_i64
          socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
          socket.write obj
        in FileRange
          len = obj.len.to_i64
          socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
          socket.write obj.to_slice
        in UInt32, Int32
          len = 4i64
          socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
          socket.write_bytes obj, IO::ByteFormat::LittleEndian
        end
        Log.debug { "Append #{len} bytes to #{@filename}" }
        len
      end
    end

    struct DeleteAction < Action
      def lag_size : Int64
        # Maybe it would be ok to not include delete action in lag, because
        # the follower should have all info necessary to GC the file during
        # startup?
        (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64
      end

      def send(socket) : Int64
        Log.debug { "Delete #{@filename}" }
        send_filename(socket)
        socket.write_bytes 0i64
        0i64
      end
    end
  end
end
