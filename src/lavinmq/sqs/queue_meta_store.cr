require "json"
require "./queue_meta"
require "../clustering/replicator"
require "../logger"

module LavinMQ
  module SQS
    # Persists the `QueueMeta` of a vhost in `sqs_queues.json` in the vhost
    # data dir. The file is replicated to followers like the other definition
    # files.
    class QueueMetaStore
      FILE_NAME = "sqs_queues.json"
      Log       = LavinMQ::Log.for "sqs.queue_meta_store"

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?, vhost : String? = nil)
        @path = File.join(@data_dir, FILE_NAME)
        @metas = Hash(String, QueueMeta).new
        @lock = Mutex.new
        @log = Logger.new(Log, vhost: vhost || "")
        load!
      end

      def []?(name : String) : QueueMeta?
        @lock.synchronize { @metas[name]? }
      end

      def set(meta : QueueMeta) : Nil
        @lock.synchronize do
          @metas[meta.name] = meta
          save!
        end
      end

      def delete(name : String) : QueueMeta?
        @lock.synchronize do
          meta = @metas.delete(name)
          save! if meta
          meta
        end
      end

      def each_value(& : QueueMeta ->) : Nil
        @lock.synchronize { @metas.each_value { |m| yield m } }
      end

      def size : Int32
        @lock.synchronize { @metas.size }
      end

      # Caller must hold @lock
      private def save! : Nil
        tmpfile = "#{@path}.tmp"
        File.open(tmpfile, "w") do |f|
          @metas.values.to_pretty_json(f)
          f.fsync
        end
        File.rename tmpfile, @path
        @replicator.try &.replace_file @path
      end

      private def load! : Nil
        return unless File.exists?(@path)
        File.open(@path) do |f|
          Array(QueueMeta).from_json(f).each { |m| @metas[m.name] = m }
          @replicator.try &.register_file f
        end
        @log.debug { "#{@metas.size} SQS queue definitions loaded" }
      rescue ex
        @log.error(exception: ex) { "Failed to load #{FILE_NAME}" }
        raise ex
      end
    end
  end
end
