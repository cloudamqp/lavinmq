require "json"
require "./parameter"
require "./logger"

module LavinMQ
  class ParameterStore(T)
    include Enumerable({ParameterId?, T})

    Log = LavinMQ::Log.for "parameter_store"

    def initialize(@data_dir : String, @file_name : String, @replicator : Clustering::Replicator, vhost : String? = nil)
      metadata = vhost ? ::Log::Metadata.build({vhost: vhost}) : ::Log::Metadata.empty
      @log = Logger.new(Log, metadata)
      @parameters = Hash(ParameterId?, T).new
      @lock = Mutex.new
      load!
    end

    def create(parameter : T, save = true)
      @lock.synchronize do
        parameters = @parameters.dup
        parameters[parameter.name] = parameter
        @parameters = parameters
        save! if save
      end
    end

    def delete(id, save = true) : T?
      @lock.synchronize do
        if @parameters.has_key? id
          parameters = @parameters.dup
          parameter = parameters.delete id
          @parameters = parameters
          save! if save
          parameter
        end
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

    def each_value(&)
      @parameters.each_value do |v|
        yield v
      end
    end

    def each_value
      @parameters.each_value
    end

    def values
      @parameters.values
    end

    def []?(id : ParameterId?) : T?
      @parameters[id]?
    end

    def [](id : ParameterId?) : T
      @parameters[id]
    end

    def has_key?(id : ParameterId?) : Bool
      @parameters.has_key? id
    end

    def to_json(json : JSON::Builder)
      json.array do
        each_value do |p|
          p.to_json(json)
        end
      end
    end

    private def save!
      @log.debug { "Saving #{@file_name}" }
      path = File.join(@data_dir, @file_name)
      tmpfile = "#{path}.tmp"
      File.open(tmpfile, "w") { |f| self.to_pretty_json(f) }
      File.rename tmpfile, path
      @replicator.replace_file path
    end

    private def load!
      path = File.join(@data_dir, @file_name)
      if File.exists?(path)
        File.open(path) do |f|
          Array(T).from_json(f).each { |p| create(p, save: false) }
          @replicator.register_file f
        end
      end
      @log.debug { "#{size} items loaded from #{@file_name}" }
    rescue ex
      @log.error(exception: ex) { "Failed to load #{@file_name}" }
      raise ex
    end
  end
end
