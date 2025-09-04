require "json"
require "log"
require "log/json"
require "../config"

module LavinMQ
  module Logging
    module Formats
      enum Format
        Json
        Logfmt
        Stdout

        def label : String
          case self
          in .json?   then "json"
          in .logfmt? then "logfmt"
          in .stdout? then "stdout"
          end
        end
      end

      abstract struct BaseFormatter < ::Log::StaticFormatter
        JOURNAL_STREAM = !!ENV["JOURNAL_STREAM"]?

        def journal? : Bool
          JOURNAL_STREAM
        end

        def single_line? : Bool
          Config.instance.log_single_line?
        end

        private def severity_to_priority
          case @entry.severity
          in ::Log::Severity::Trace  then 7 # journald doesn't have trace
          in ::Log::Severity::Debug  then 7
          in ::Log::Severity::Info   then 6
          in ::Log::Severity::Notice then 5
          in ::Log::Severity::Warn   then 4
          in ::Log::Severity::Error  then 3
          in ::Log::Severity::Fatal  then 2
          in ::Log::Severity::None   then 6
          end
        end

        def journal_severity(io : IO = @io) : Nil
          @io << '<' << severity_to_priority << '>'
        end
      end

      struct JsonFormat < BaseFormatter
        def run
          if journal?
            journal_severity
          end
          JSON.build(@io) do |json|
            json.object do
              json.string "timestamp"
              timestamp json
              json.field "severity", @entry.severity.label
              json.field "source", @entry.source if @entry.source
              if (context = @entry.context) && !context.empty?
                json.field "context" { context.to_json json }
              end
              json.field "message", @entry.message if @entry.message
              if (data = @entry.data) && !data.empty?
                json.field "data" { data.to_json json }
              end
              exception json
            end
          end
        end

        private def exception(json)
          if exception = @entry.exception
            json.field "exception" do
              json.object do
                json.field "class", exception.class.name
                json.field "message", exception.message
                if backtrace = exception.backtrace?
                  json.field "backtrace" do
                    backtrace.to_json json
                  end
                end
              end
            end
          end
        end

        private def timestamp(json)
          json.string do |io|
            @entry.timestamp.to_rfc3339(io, fraction_digits: 6)
          end
        end
      end

      struct LogfmtFormat < BaseFormatter
        def run
          if journal?
            journal_severity
          else
            timestamp "ts="
            severity " at="
          end
          source " src="
          context ' '
          message " msg="
          data ' '
          exception " error=", "backtrace="
        end

        def timestamp(before = nil)
          @io << before << super()
        end

        def severity(before = nil)
          @io << before << @entry.severity.label.downcase
        end

        def source(before = nil)
          @io << before << @entry.source if @entry.source
        end

        def message(before = nil)
          if message = @entry.message
            need_quotes = message.includes? ' '
            @io << before
            @io << '"' if need_quotes
            @io << @entry.message
            @io << '"' if need_quotes
          end
        end

        def context(before = nil)
          metadata_to_s(@entry.context, before) if @entry.context
        end

        def data(before = nil)
          metadata_to_s(@entry.data, before) if @entry.data
        end

        def exception(before = nil, before_backtrace = nil)
          if ex = @entry.exception
            # @io << before << '"' << ex.class.name << ": " << ex.message << '"'
            if ex.backtrace?
              if single_line?
                @io << ' ' << before_backtrace << '"' << ex.backtrace.join(@io, "\\n") << '"'
              else
                backtrace_id = Time.monotonic.nanoseconds
                @io << " " << before_backtrace
                backtrace_id.to_s(@io, 32)
                multi_line_backtrace(before_backtrace, backtrace_id, ex)
              end
            end
          end
        end

        private def multi_line_backtrace(before_backtrace, backtrace_id, exception : Exception, heading = "from")
          return unless backtrace = exception.backtrace?
          @io << "\n" << before_backtrace
          backtrace_id.to_s(@io, 32)
          @io << " -- " << heading << " " << exception.class.name << ": " << exception.message
          backtrace.each do |line|
            @io << "\n" << before_backtrace
            backtrace_id.to_s(@io, 32)
            @io << "  "
            @io << line
          end
          if cause = exception.cause
            multi_line_backtrace(before_backtrace, backtrace_id, cause, "caused by")
          end
        end

        private def metadata_to_s(metadata : ::Log::Metadata, before = nil) : Nil
          return if metadata.empty?
          @io << before
          found = false
          metadata.each do |key, value|
            @io << ' ' if found
            @io << key.to_s << '=' << value.to_s
            found = true
          end
        end
      end

      struct StdoutFormat < BaseFormatter
        def run
          if journal?
            journal_severity
          else
            timestamp
            severity
          end
          source(before: ' ')
          context(before: '[', after: ']')
          string ' '
          message
          data(before: '[', after: ']')
          exception
        end
      end
    end
  end
end
