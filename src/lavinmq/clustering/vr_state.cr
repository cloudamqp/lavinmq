require "json"
require "./durable_file"

module LavinMQ
  module Clustering
    class VRState
      FILE_NAME               = ".clustering_vr_state"
      OP_NUMBER_FILE_NAME     = "#{FILE_NAME}.op_number"
      COMMIT_NUMBER_FILE_NAME = "#{FILE_NAME}.commit_number"
      COMPACT_BYTES           = 8_i64 * 1024 * 1024
      COUNTER_BYTES           = 8_i64

      getter path : String

      @lock = Mutex.new(:unchecked)
      @view : Int64 = 0_i64
      @role : String = "backup"
      @op_number = Atomic(Int64).new(0_i64)
      @commit_number : Int64 = 0_i64
      @voted_view : Int64 = -1_i64
      @voted_for : Int32?
      @op_path : String
      @commit_path : String
      @op_file : File?
      @commit_file : File?
      @op_bytes_written = 0_i64
      @commit_bytes_written = 0_i64
      @op_needs_compaction = false
      @commit_needs_compaction = false
      @compact_bytes : Int64

      def initialize(data_dir : String, compact_bytes : Int64 = COMPACT_BYTES)
        @compact_bytes = compact_bytes
        @path = File.join(data_dir, FILE_NAME)
        @op_path = File.join(data_dir, OP_NUMBER_FILE_NAME)
        @commit_path = File.join(data_dir, COMMIT_NUMBER_FILE_NAME)
        restore
      end

      def view : Int64
        @lock.synchronize { @view }
      end

      def role : String
        @lock.synchronize { @role }
      end

      def op_number : Int64
        @op_number.get(:relaxed)
      end

      def commit_number : Int64
        @lock.synchronize { @commit_number }
      end

      def role=(role : String) : Nil
        @lock.synchronize do
          @role = role
          store_state
        end
      end

      def advance_view! : Int64
        @lock.synchronize do
          @view += 1
          @role = "backup"
          store_state
          @view
        end
      end

      def next_op! : Int64
        @lock.synchronize do
          op = @op_number.get(:relaxed) + 1_i64
          @op_number.set(op, :relaxed)
          append_op_number(op)
          op
        end
      end

      def apply_op!(op : Int64) : Nil
        @lock.synchronize do
          return if op <= @op_number.get(:relaxed)

          @op_number.set(op, :relaxed)
          append_op_number(op)
        end
      end

      def commit!(op : Int64) : Nil
        @lock.synchronize do
          return if op <= @commit_number
          @commit_number = op
          append_commit_number(op)
        end
      end

      def to_named_tuple(primary_id : Int32, primary_uri : String, node_id : Int32, quorum_size : Int32)
        @lock.synchronize do
          {
            backend:       "vr",
            node_id:       node_id,
            role:          @role,
            view:          @view,
            primary_id:    primary_id,
            primary_uri:   primary_uri,
            op_number:     @op_number.get(:relaxed),
            commit_number: @commit_number,
            quorum_size:   quorum_size,
          }
        end
      end

      def grant_vote?(candidate_id : Int32, requested_view : Int64, candidate_op : Int64, primary_id : Int32) : Bool
        @lock.synchronize do
          changed = false
          if requested_view > @view
            @view = requested_view
            @role = "backup"
            changed = true
          end

          granted = false
          if requested_view == @view && candidate_id == primary_id && candidate_op >= @op_number.get(:relaxed)
            if @voted_view == requested_view
              granted = @voted_for == candidate_id
            else
              @voted_view = requested_view
              @voted_for = candidate_id
              changed = true
              granted = true
            end
          end

          store_state if changed
          granted
        end
      end

      def grant_authority?(primary_id : Int32, requested_view : Int64, expected_primary_id : Int32) : Bool
        @lock.synchronize do
          changed = false
          if requested_view > @view
            @view = requested_view
            @role = "backup"
            changed = true
          end

          granted = requested_view == @view &&
                    primary_id == expected_primary_id &&
                    @voted_view <= requested_view
          store_state if changed
          granted
        end
      end

      def observe_view!(seen_view : Int64) : Nil
        @lock.synchronize do
          return if seen_view <= @view

          @view = seen_view
          @role = "backup"
          store_state
        end
      end

      def voted_view : Int64
        @lock.synchronize { @voted_view }
      end

      def voted_for : Int32?
        @lock.synchronize { @voted_for }
      end

      private def restore : Nil
        restore_state
        restore_op_number
        restore_commit_number
      end

      private def restore_state : Nil
        return unless File.exists?(@path)

        restore_state_json(JSON.parse(File.read(@path)))
      rescue ex : IO::Error | JSON::ParseException | KeyError | TypeCastError
        raise Error.new("Invalid VR state file #{@path}: #{ex.message}")
      end

      private def restore_state_json(json : JSON::Any) : Nil
        @view = json["view"].as_i64
        @role = json["role"].as_s
        @voted_view = json["voted_view"]?.try(&.as_i64) || -1_i64
        @voted_for = nil
        if voted_for = json["voted_for"]?
          @voted_for = voted_for.raw.nil? ? nil : voted_for.as_i
        end
      end

      private def restore_op_number : Nil
        counter = restore_counter(@op_path)
        if value = counter[:value]
          @op_number.set(value, :relaxed)
        end
        @op_bytes_written = counter[:bytes_written]
        @op_needs_compaction = counter[:needs_compaction]
      end

      private def restore_commit_number : Nil
        counter = restore_counter(@commit_path)
        if value = counter[:value]
          @commit_number = value
        end
        @commit_bytes_written = counter[:bytes_written]
        @commit_needs_compaction = counter[:needs_compaction]
      end

      private def restore_counter(path : String)
        return {value: nil.as(Int64?), bytes_written: 0_i64, needs_compaction: false} unless File.exists?(path)

        size = File.size(path)
        complete_size = size - (size % COUNTER_BYTES)
        value = nil.as(Int64?)
        if complete_size > 0
          File.open(path) do |file|
            file.seek(complete_size - COUNTER_BYTES)
            value = file.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
        end
        {value: value, bytes_written: size, needs_compaction: complete_size != size}
      rescue ex : IO::Error
        raise Error.new("Invalid VR counter file #{path}: #{ex.message}")
      end

      private def store_state : Nil
        DurableFile.replace(@path, File::DEFAULT_CREATE_PERMISSIONS.value.to_i32) do |io|
          {
            view:       @view,
            role:       @role,
            voted_view: @voted_view,
            voted_for:  @voted_for,
          }.to_json(io)
        end
      end

      private def append_op_number(op : Int64) : Nil
        if @op_needs_compaction || (@op_bytes_written > 0 && @op_bytes_written + COUNTER_BYTES >= @compact_bytes)
          close_op_file
          @op_file = compact_counter(@op_path, op)
          @op_bytes_written = COUNTER_BYTES
          @op_needs_compaction = false
        else
          append_counter(op_file, op)
          @op_bytes_written += COUNTER_BYTES
        end
      end

      private def append_commit_number(op : Int64) : Nil
        if @commit_needs_compaction || (@commit_bytes_written > 0 && @commit_bytes_written + COUNTER_BYTES >= @compact_bytes)
          close_commit_file
          @commit_file = compact_counter(@commit_path, op)
          @commit_bytes_written = COUNTER_BYTES
          @commit_needs_compaction = false
        else
          append_counter(commit_file, op)
          @commit_bytes_written += COUNTER_BYTES
        end
      end

      private def append_counter(file : File, value : Int64) : Nil
        file.write_bytes(value, IO::ByteFormat::LittleEndian)
      end

      private def compact_counter(path : String, value : Int64) : File
        tmp_path = "#{path}.tmp"

        Dir.mkdir_p(File.dirname(path))
        file = File.open(tmp_path, "w")
        begin
          file.sync = true
          file.write_bytes(value, IO::ByteFormat::LittleEndian)
          File.rename(tmp_path, path)
          file
        rescue ex
          file.close unless file.closed?
          File.delete?(tmp_path)
          raise ex
        end
      end

      private def op_file : File
        @op_file ||= open_counter_file(@op_path)
      end

      private def commit_file : File
        @commit_file ||= open_counter_file(@commit_path)
      end

      private def open_counter_file(path : String) : File
        Dir.mkdir_p(File.dirname(path))
        File.open(path, "a").tap do |file|
          file.sync = true
        end
      end

      private def close_op_file : Nil
        if file = @op_file
          file.close
          @op_file = nil
        end
      end

      private def close_commit_file : Nil
        if file = @commit_file
          file.close
          @commit_file = nil
        end
      end

      class Error < Exception; end
    end
  end
end
