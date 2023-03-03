require "../controller"
require "../../config"
require "../../in_memory_backend"

module LavinMQ
  module HTTP
    class LogsController < Controller
      LogBackend = ::Log::InMemoryBackend.instance

      private def register_routes
        static_view "/logs"

        get "/api/livelog" do |context, _params|
          channel = LogBackend.add_channel
          context.response.content_type = "text/event-stream"
          context.response.upgrade do |io|
            if last_event_id = context.request.headers["Last-Event-Id"]?
              last_ts = last_event_id.to_i64
              LogBackend.entries.each do |entry|
                next if entry.timestamp.to_unix_ms <= last_ts
                print_entry(entry, io)
              end
            else
              LogBackend.entries.each do |entry|
                print_entry(entry, io)
              end
            end
            io.flush
            while entry = channel.receive
              print_entry(entry, io)
              io.flush
            end
          ensure
            LogBackend.remove_channel(channel)
          end
          context
        end

        get "/api/logs" do |context, _params|
          context.response.content_type = "text/plain"
          context.response.headers["Content-Disposition"] = "attachment; filename=logs.txt"
          LogBackend.entries.each do |e|
            context.response.puts "#{e.timestamp} [#{e.severity.to_s.upcase}] #{e.source} - #{e.message}"
          end
          context
        end
      end

      private def print_entry(entry, io)
        io.print("id: #{entry.timestamp.to_unix_ms}\n")
        io.print("data: ")
        {entry.severity, entry.source, entry.message}.to_json(io)
        io.print("\n\n")
      end
    end
  end
end
