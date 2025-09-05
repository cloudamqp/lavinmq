require "log/metadata"

module LavinMQ
  module Logging
    module Loggable
      @log_context : ::Log::Metadata = ::Log::Metadata.empty

      def _set_log_context(new_metadata : ::Log::Metadata)
        @log_context = new_metadata
      end

      def _set_log_context(new_metadata : NamedTuple)
        @log_context = ::Log::Metadata.build(new_metadata)
      end

      def log_context
        @log_context
      end
    end
  end
end
