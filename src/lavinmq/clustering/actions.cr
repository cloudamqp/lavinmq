require "../mfile"

module LavinMQ
  module Clustering
    abstract struct Action
      def initialize(@data_dir : String, @filename : String)
      end

      abstract def lag_size : Int64
      abstract def send(socket : IO, log = Log) : Int64
      abstract def done

      getter filename

      private def send_filename(socket : IO)
        socket.write_bytes filename.bytesize.to_i32, IO::ByteFormat::LittleEndian
        socket.write filename.to_slice
      end
    end

    struct ReplaceAction < Action
      def initialize(@data_dir : String, @filename : String, @mfile : MFile? = nil)
      end

      def lag_size : Int64
        if mfile = @mfile
          0i64 + sizeof(Int32) + @filename.bytesize +
            sizeof(Int64) + mfile.size.to_i64
        else
          0i64 + sizeof(Int32) + @filename.bytesize +
            sizeof(Int64) + File.size(File.join(@data_dir, @filename)).to_i64
        end
      end

      def send(socket, log = Log) : Int64
        log.debug { "Replace #{@filename}" }
        send_filename(socket)
        if mfile = @mfile
          size = mfile.size.to_i64
          socket.write_bytes size, IO::ByteFormat::LittleEndian
          socket.write mfile.to_slice(0, size)
          mfile.dontneed
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

      def done
      end
    end

    struct AppendAction < Action
      def initialize(@data_dir : String, @filename : String, @obj : Bytes | UInt32 | Int32)
      end

      def lag_size : Int64
        datasize = case obj = @obj
                   in Bytes
                     obj.bytesize.to_i64
                   in UInt32, Int32
                     4i64
                   end
        0i64 + sizeof(Int32) + @filename.bytesize +
          sizeof(Int64) + datasize
      end

      def send(socket, log = Log) : Int64
        send_filename(socket)
        len : Int64
        case obj = @obj
        in Bytes
          len = obj.bytesize.to_i64
          socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
          socket.write obj
        in UInt32, Int32
          len = 4i64
          socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
          socket.write_bytes obj, IO::ByteFormat::LittleEndian
        end
        log.debug { "Append #{len} bytes to #{@filename}" }
        len
      end

      def done
      end
    end

    struct DeleteAction < Action
      def initialize(@data_dir : String, @filename : String, @wg : WaitGroup)
        @wg.add
      end

      def lag_size : Int64
        # Maybe it would be ok to not include delete action in lag, because
        # the follower should have all info necessary to GC the file during
        # startup?
        (sizeof(Int32) + @filename.bytesize + sizeof(Int64)).to_i64
      end

      def send(socket, log = Log) : Int64
        log.debug { "Delete #{@filename}" }
        send_filename(socket)
        socket.write_bytes 0i64
        0i64
      ensure
        done
      end

      def done
        @wg.done
      end
    end
  end
end
