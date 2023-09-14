require "../stdlib/log_entry"
require "./logging/*"

module LavinMQ
  Log = ::Log.for "lavinmq"

  module Logging
    #
    # Wraps a ::Log instance and appends metadata to all
    # log entries.
    #
    struct EntityLog
      getter log : ::Log
      getter metadata : ::Log::Metadata

      def self.new(log : ::Log, **metadata)
        new(log, metadata)
      end

      def self.new(log : ::Log, metadata : ::Log::Metadata)
        new(log, metadata)
      end

      protected def initialize(@log : ::Log, metadata : NamedTuple = NamedTuple.new)
        @metadata = ::Log::Metadata.build(metadata)
      end

      protected def initialize(@log : ::Log, @metadata : ::Log::Metadata)
      end

      # Create a new EntityLog with the same Log, but with extended metadata
      def extend(**kwargs)
        self.extend(self.log, **kwargs)
      end

      # Create a new EntityLog with another Log, but with extended metadata
      def extend(log : ::Log, **kwargs)
        metadata = ::Log::Metadata.extend(self.metadata, kwargs)
        EntityLog.new(log, metadata)
      end

      {% for method in %w(trace debug info notice warn error fatal) %}
        def {{method.id}}(*, exception : Exception? = nil, &)
          @log.{{method.id}}(exception: exception) do |emitter|
            result = yield
            unless result.nil?
              emitter.emit result, @metadata
            end
          end
        end
      {% end %}
    end
  end
end

# Add an extend method to Metadata that will copy all values from parent
# to get parent values first. This will also remove the need from defrag
# (which is done from #each).
class Log::Metadata
  def self.extend(parent : Metadata, entries : NamedTuple = NamedTuple.new)
    if entries.empty?
      return parent
    end
    data_size = instance_sizeof(self) + sizeof(Entry) * (entries.size + parent.@size)
    data = GC.malloc(data_size).as(self)
    data.setup_extend(parent, entries)
    data
  end

  protected def setup_extend(parent : Metadata, entries : NamedTuple)
    @size = @overridden_size = @max_total_size = entries.size + parent.@size
    ptr_entries = pointerof(@first)
    ptr_entries.copy_from(pointerof(parent.@first), parent.@size)
    ptr_entries += parent.size
    entries.each_with_index do |key, value, i|
      ptr_entries[i] = {key: key, value: Value.to_metadata_value(value)}
    end
  end
end
