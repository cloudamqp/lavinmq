require "./scheduled_job"

module LavinMQ
  class ScheduledJobStore
    def initialize(@vhost : VHost)
      @jobs = Hash(String, ScheduledJob::Runner).new
    end

    def create(name : String, config : JSON::Any)
      @jobs[name]?.try &.close

      exchange = config["exchange"]?.try(&.as_s) || raise JSON::Error.new("Field 'exchange' is required")
      routing_key = config["routing_key"]?.try(&.as_s) || raise JSON::Error.new("Field 'routing_key' is required")
      body = config["body"]?.try(&.as_s) || raise JSON::Error.new("Field 'body' is required")
      schedule = config["schedule"]?.try(&.as_s) || raise JSON::Error.new("Field 'schedule' is required")
      enabled = config["enabled"]?.try(&.as_bool) || true

      job = ScheduledJob::Runner.new(name, @vhost, exchange, routing_key, body, schedule, enabled)
      @jobs[name] = job
      job
    rescue ex : KeyError
      raise JSON::Error.new("Missing required field: #{ex.message}")
    end

    def delete(name : String)
      if job = @jobs.delete(name)
        job.close
        job
      end
    end

    def [](name : String)
      @jobs[name]
    end

    def []?(name : String)
      @jobs[name]?
    end

    def each(&)
      @jobs.each do |name, job|
        yield name, job
      end
    end

    def each_value
      @jobs.each_value
    end

    def close
      @jobs.each_value(&.close)
      @jobs.clear
    end
  end
end
