require "json"
require "./parameter"
require "./logger"

module LavinMQ
  class ParameterStore(T)
    include Enumerable({ParameterId?, T})

    Log = LavinMQ::Log.for "parameter_store"

    @save_lock = Mutex.new

    def initialize(@data_dir : String, @file_name : String, @replicator : Clustering::Replicator?, vhost : String? = nil)
      metadata = vhost ? ::Log::Metadata.build({vhost: vhost}) : ::Log::Metadata.empty
      @log = Logger.new(Log, metadata)
      @parameters = Hash(ParameterId?, T).new
      load!
    end

    def []?(id) : T?
      @parameters[id]?
    end

    def [](id) : T
      @parameters[id]
    end

    def each_value(& : T ->) : Nil
      @parameters.each_value { |p| yield p }
    end

    def has_key?(id) : Bool
      @parameters.has_key?(id)
    end

    def size : Int32
      @parameters.size
    end

    def values : Array(T)
      @parameters.values
    end

    def empty? : Bool
      @parameters.empty?
    end

    def create(parameter : T, save = true)
      @parameters[parameter.name] = parameter
      save! if save
    end

    def delete(id, save = true) : T?
      if parameter = @parameters.delete id
        save! if save
        parameter
      end
    end

    def apply(parameter : Parameter? = nil, &)
      itr = if parameter.nil?
              @parameters.each_value
            else
              [parameter].each
            end
      itr.each do |p|
        yield p
      rescue ex : Exception
        @log.error { "Parameter #{p.component_name}/#{p.parameter_name} could not be applied with value=#{p.value} error='#{ex.message}'" }
        delete(p.name)
        raise ex unless parameter.nil?
      end
    end

    def each(&)
      @parameters.each do |kv|
        yield kv
      end
    end

    def to_json(json : JSON::Builder)
      json.array do
        each_value do |p|
          p.to_json(json)
        end
      end
    end

    def save!
      @log.debug { "Saving #{@file_name}" }
      path = File.join(@data_dir, @file_name)
      tmpfile = "#{path}.tmp"
      # Serialize saves so concurrent add/delete don't race on the shared `.tmp`
      # file and fail the rename.
      @save_lock.synchronize do
        File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
        File.rename tmpfile, path
      end
      @replicator.try do |r|
        r.replace_file path
        # Acknowledge the policy/parameter change only once a quorum has it
        # durably, so it survives a leader failover. See VHostStore#save!.
        r.wait_for_followers
      end
    end

    private def load!
      path = File.join(@data_dir, @file_name)
      if File.exists?(path)
        File.open(path) do |f|
          Array(T).from_json(f).each { |p| create(p, save: false) }
          @replicator.try &.register_file f
        end
      end
      @log.debug { "#{size} items loaded from #{@file_name}" }
    rescue ex
      @log.error(exception: ex) { "Failed to load #{@file_name}" }
      raise ex
    end
  end
end
