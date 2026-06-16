require "json"

module LavinMQ
  module Clustering
    class VRState
      FILE_NAME = ".clustering_vr_state"

      getter path : String

      @lock = Mutex.new(:unchecked)
      @view : Int64 = 0_i64
      @role : String = "backup"
      @op_number : Int64 = 0_i64
      @commit_number : Int64 = 0_i64

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
        @lock.synchronize { @op_number }
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
          @op_number += 1
          @op_number
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
            op_number:     @op_number,
            commit_number: @commit_number,
            quorum_size:   quorum_size,
          }
        end
      end

      private def restore : Nil
        return unless File.exists?(@path)

        json = JSON.parse(File.read(@path))
        @view = json["view"].as_i64
        @role = json["role"].as_s
        @op_number = json["op_number"].as_i64
        @commit_number = json["commit_number"].as_i64
      rescue ex : JSON::ParseException | KeyError | TypeCastError
        raise Error.new("Invalid VR state file #{@path}: #{ex.message}")
      end

      private def store : Nil
        Dir.mkdir_p(File.dirname(@path))
        File.open(@path, "w") do |io|
          {
            view:          @view,
            role:          @role,
            op_number:     @op_number,
            commit_number: @commit_number,
          }.to_json(io)
        end
      end

      class Error < Exception; end
    end
  end
end
