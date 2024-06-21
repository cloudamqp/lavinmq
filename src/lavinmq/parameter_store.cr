require "json"
require "./parameter"

module LavinMQ
  class ParameterStore(T)
    include Enumerable({ParameterId?, T})

    Log = ::Log.for("parameterstore")

    def initialize(@data_dir : String, @file_name : String, @replicator : Clustering::Replicator, vhost : String? = nil)
      @metadata = vhost ? ::Log::Metadata.new(nil, {vhost: vhost}) : ::Log::Metadata.empty
      @parameters = Hash(ParameterId?, T).new
      load!
    end

    forward_missing_to @parameters

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
        Log.error &.emit "Parameter #{p.component_name}/#{p.parameter_name} could not be applied with value=#{p.value} error='#{ex.message}'", @metadata
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

    private def save!
      Log.debug &.emit "Saving #{@file_name}", @metadata
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
      Log.debug &.emit "#{size} items loaded from #{@file_name}", @metadata
    end
  end
end
