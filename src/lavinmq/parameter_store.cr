require "json"
require "./parameter"
require "./logging"

module LavinMQ
  class ParameterStore(T)
    include Enumerable({ParameterId?, T})
    include Logging::Loggable

    Log = LavinMQ::Log.for "parameter_store"

    def initialize(@data_dir : String, @file_name : String, @replicator : Clustering::Replicator, vhost : String? = nil)
      L.set_metadata(vhost: vhost)
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
        L.error "Parameter could not be applied", exception: ex,
          component: p.component_name, parameter: p.parameter_name, value: p.value.to_s
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
      L.debug "Saving parameters", file_name: @file_name
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
      L.debug "Parameters loaded", file_name: @file_name, items: size
    rescue ex
      L.error "Failed to load parameters", file_name: @file_name, exception: ex
      raise ex
    end
  end
end
