require "json"

module Booth
  # Timeline of things worth telling visitors about: nodes going away and
  # coming back, leader elections and how long clients took to recover.
  class Events
    record Event, at : Time, kind : String, message : String do
      def to_json(json : JSON::Builder)
        json.object do
          json.field "at", at.to_unix_ms
          json.field "kind", kind
          json.field "message", message
        end
      end
    end

    # A window where the integrity publisher got no confirms
    record Outage, from : Time, to : Time do
      def to_json(json : JSON::Builder)
        json.object do
          json.field "from", from.to_unix_ms
          json.field "to", to.to_unix_ms
        end
      end
    end

    MAX = 50

    getter events = Deque(Event).new
    getter outages = Deque(Outage).new

    def add(kind : String, message : String) : Nil
      @events.shift if @events.size >= MAX
      @events << Event.new(Time.utc, kind, message)
      Log.info { message }
    end

    def outage(from : Time, to : Time) : Nil
      @outages.shift if @outages.size >= MAX
      @outages << Outage.new(from, to)
    end

    def clear : Nil
      @events.clear
      @outages.clear
    end

    def to_json_fields(json : JSON::Builder)
      json.field "events", @events
      json.field "outages", @outages
    end
  end
end
