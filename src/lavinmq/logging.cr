require "../stdlib/log_entry"
require "./logging/*"

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

      # Create a new EntityLog with the same Log, but with extended metadata
      def extend(**kwargs)
        data = self.metadata.to_h
        unless kwargs.empty?
          kwhash = kwargs.to_h.transform_values do |v|
            ::Log::Metadata::Value.new(v).as(::Log::Metadata::Value)
          end
          data.merge!(kwhash)
        end
        EntityLog.new(self.log, ::Log::Metadata.build(data))
      end

      # Create a new EntityLog with another Log, but with extended metadata
      def extend(log : ::Log, **kwargs)
        data = self.metadata.to_h
        unless kwargs.empty?
          kwhash = kwargs.to_h.transform_values do |v|
            ::Log::Metadata::Value.new(v).as(::Log::Metadata::Value)
          end
          data.merge!(kwhash)
        end
        EntityLog.new(log, ::Log::Metadata.build(data))
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
