require "log"

module LavinMQ
  struct Logger
    def initialize(@log : ::Log, @metadata : ::Log::Metadata)
    end

    def initialize(@log : ::Log, **kwargs)
      if kwargs.empty?
        @metadata = ::Log::Metadata.empty
      else
        @metadata = ::Log::Metadata.build(kwargs)
      end
    end

    {% for method in %w(trace debug info notice warn error fatal) %}
      def {{method.id}}(exception : Exception? = nil, &block)
        severity = ::Log::Severity::{{method.camelcase.id}}
        return unless @log.level <= severity
        return unless backend = @log.backend
        return unless logstr = yield.to_s
        backend.dispatch ::Log::Entry.new(@log.source, severity, logstr, @metadata, exception, timestamp: Time.local)
      end
    {% end %}
  end
end
