require "log"
require "json"

module LavinMQ
  module Logging
    module Format
      abstract struct BaseFormatter < ::Log::StaticFormatter
        JOURNAL_STREAM = !!ENV["JOURNAL_STREAM"]?

        def self.format(entry : ::Log::Entry, io : IO)
          fmt = new(entry, io)
          fmt.run(JOURNAL_STREAM)
        end

        def run
          raise "dont use this"
        end

        abstract def run(journal : Bool)

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

        def journal_severity : Nil
          @io << '<' << severity_to_priority << '>'
        end
      end

      struct JsonFormat < BaseFormatter
        def run(journal)
          if journal
            journal_severity
          end
          JSON.build(@io) do |json|
            json.object do
              json.field "timestamp" do
                json.string { |io| @entry.timestamp.to_rfc3339(io, fraction_digits: 6) }
              end
              json.field "severity", @entry.severity.label
              json.field "source", @entry.source if @entry.source
              if (context = @entry.context) && !context.empty?
                context.each do |key, value|
                  json.string key
                  json.string value
                end
              end
              json.field "message", @entry.message if @entry.message
              if (data = @entry.data) && !data.empty?
                data.each do |key, value|
                  json.string key
                  json.string value
                end
              end
            end
          end
        end
      end

      struct LogfmtFormat < BaseFormatter
        def run(journal)
          if journal
            journal_severity
          else
            timestamp
            severity
          end
          source
          context
          message
          data
        end

        def timestamp
          @io << "ts=" << super << ' '
        end

        def severity(journal : Bool)
          @io << "at=" << @entry.severity.label.downcase << ' '
        end

        def source
          @io << "src=" << @entry.source << ' ' if @entry.source
        end

        def message
          if message = @entry.message
            quote = message.includes? ' '
            @io << "msg="
            @io << '"' if quote
            @io << @entry.message
            @io << '"' if quote
            @io << ' '
          end
        end

        def context
          metadata_to_s(@entry.context) if @entry.context
        end

        def data
          metadata_to_s(@entry.data) if @entry.data
        end

        private def metadata_to_s(metadata : ::Log::Metadata) : Nil
          return if metadata.empty?
          found = false
          metadata.each do |key, value|
            @io << ' ' if found
            @io << key.to_s << '=' << value.to_s
            found = true
          end
          @io << ' '
        end
      end

      struct StdoutFormat < BaseFormatter
        def run(journal)
          if journal
            journal_severity
          else
            timestamp
            severity
          end
          source(before: ' ')
          context(before: '[', after: ']')
          data(before: '[', after: ']')
          string ' '
          message
          exception
        end
      end
    end
  end
end
