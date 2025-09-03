require "log"
require "json"

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

        def journal_severity(io : IO = @io) : Nil
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
            string ' '
          else
            timestamp ' '
            severity ' '
          end
          source ' '
          context ' '
          message ' '
          data
        end

        def timestamp(after = nil)
          @io << "ts=" << super() << after
        end

        def severity(after = nil)
          @io << "at=" << @entry.severity.label.downcase << after
        end

        def source(after = nil)
          @io << "src=" << @entry.source << after if @entry.source
        end

        def message(after = nil)
          if message = @entry.message
            need_quotes = message.includes? ' '
            @io << "msg="
            @io << '"' if need_quotes
            @io << @entry.message
            @io << '"' if need_quotes
            @io << after
          end
        end

        def context(after = nil)
          metadata_to_s(@entry.context, after) if @entry.context
        end

        def data(after = nil)
          metadata_to_s(@entry.data, after) if @entry.data
        end

        private def metadata_to_s(metadata : ::Log::Metadata, after = nil) : Nil
          return if metadata.empty?
          found = false
          metadata.each do |key, value|
            @io << ' ' if found
            @io << key.to_s << '=' << value.to_s
            found = true
          end
          @io << after
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
          string ' '
          message
          data(before: '[', after: ']')
          exception
        end
      end
    end
  end
end
