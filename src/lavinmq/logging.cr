require "log"

module LavinMQ
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

      def extend(**kwargs)
        EntityLog.new(self.log, self.metadata.extend(kwargs))
      end

      def for(source, **kwargs)
        EntityLog.new(self.log.for(source), self.metadata.extend(kwargs))
      end

      {% for method in %w(trace debug info notice warn error fatal) %}
        def {{method.id}}(*, exception : Exception? = nil, &)
          @log.{{method.id}}(exception: exception) do |emitter|
            emitter.emit yield, @metadata
          end
        end
      {% end %}
    end
  end
end
