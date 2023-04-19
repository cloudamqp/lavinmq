require "log"

module LavinMQ
  module Logging
    #
    # Wraps a ::Log instance and appends metadata to all
    # log entries.
    #
    struct EntityLog
      getter log : ::Log

      # This is syntax suger so one doesn't have to pass a NamedTuple
      # (and splats can't be used in constructor?)
      def self.new(log : ::Log, **metadata)
        new(log, metadata)
      end

      @metadata : ::Log::Metadata

      private def initialize(@log : ::Log, metadata : NamedTuple = NamedTuple.new)
        @metadata = ::Log::Metadata.build(metadata)
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
