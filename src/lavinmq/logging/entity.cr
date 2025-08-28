require "log"
require "./logger"

module LavinMQ
  module Logging
    module Entity
      @_log_entity_metadata : ::Log::Metadata = ::Log::Metadata.empty

      def setup_log(metadata : NamedTuple)
        @_log_entity_metadata = ::Log::Metadata.build(metadata)
      end

      def log_metadata
        @_log_entity_metadata
      end

      module MyLog
        {% for method in %w(trace debug info notice warn error fatal) %}
            macro {{method.id}}(msg, exception = nil, **kwargs)
              Log.{{method.id}}(exception: \{{exception}}) do |emitter|
                \{% if kwargs.empty? %}
                  emitter.emit(\{{msg}}, @_log_entity_metadata)
                \{% else %}
                  emitter.emit(\{{msg}}, @_log_entity_metadata.extend(\{{kwargs}}))
                \{% end %}
              end
            end
          {% end %}
      end
    end
  end
end
