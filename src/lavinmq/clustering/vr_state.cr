require "json"
require "./durable_file"

module LavinMQ
  module Clustering
    class VRState
      FILE_NAME = ".clustering_vr_state"

      getter path : String

      @lock = Mutex.new(:unchecked)
      @view : Int64 = 0_i64
      @role : String = "backup"
      @op_number = Atomic(Int64).new(0_i64)
      @commit_number : Int64 = 0_i64
      @voted_view : Int64 = -1_i64
      @voted_for : Int32?

      def initialize(data_dir : String)
        @path = File.join(data_dir, FILE_NAME)
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
          store
        end
      end

      def advance_view! : Int64
        @lock.synchronize do
          @view += 1
          @role = "backup"
          store
          @view
        end
      end

      def next_op! : Int64
        @lock.synchronize do
          op = @op_number.get(:relaxed) + 1_i64
          @op_number.set(op, :relaxed)
          store
          op
        end
      end

      def apply_op!(op : Int64) : Nil
        @lock.synchronize do
          return if op <= @op_number.get(:relaxed)

          @op_number.set(op, :relaxed)
          store
        end
      end

      def commit!(op : Int64) : Nil
        @lock.synchronize do
          return if op <= @commit_number
          @commit_number = op
          store
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

          store if changed
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
          store if changed
          granted
        end
      end

      def observe_view!(seen_view : Int64) : Nil
        @lock.synchronize do
          return if seen_view <= @view

          @view = seen_view
          @role = "backup"
          store
        end
      end

      def voted_view : Int64
        @lock.synchronize { @voted_view }
      end

      def voted_for : Int32?
        @lock.synchronize { @voted_for }
      end

      private def restore : Nil
        return unless File.exists?(@path)

        json = JSON.parse(File.read(@path))
        @view = json["view"].as_i64
        @role = json["role"].as_s
        @op_number.set(json["op_number"].as_i64, :relaxed)
        @commit_number = json["commit_number"].as_i64
        @voted_view = json["voted_view"]?.try(&.as_i64) || -1_i64
        if voted_for = json["voted_for"]?
          @voted_for = voted_for.raw.nil? ? nil : voted_for.as_i
        end
      rescue ex : JSON::ParseException | KeyError | TypeCastError
        raise Error.new("Invalid VR state file #{@path}: #{ex.message}")
      end

      private def store : Nil
        DurableFile.replace(@path) do |io|
          {
            view:          @view,
            role:          @role,
            op_number:     @op_number.get(:relaxed),
            commit_number: @commit_number,
            voted_view:    @voted_view,
            voted_for:     @voted_for,
          }.to_json(io)
        end
      end

      class Error < Exception; end
    end
  end
end
