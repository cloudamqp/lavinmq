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

      macro for(**kwargs)
        ::LavinMQ::Logging::EntityLog.new(Log, {{**kwargs}})
      end

      def self.new(log : ::Log, **metadata)
        new(log, metadata)
      end

      private def initialize(@log : ::Log, metadata : NamedTuple = NamedTuple.new)
        @metadata = ::Log::Metadata.build(metadata)
      end

      def extend(**kwargs)
        new(self.log, self.metadata.extend(kwargs))
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
