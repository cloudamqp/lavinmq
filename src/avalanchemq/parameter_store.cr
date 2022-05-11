require "json"
require "./parameter"

module LavinMQ
  class ParameterStore(T)
    include Enumerable({ParameterId?, T})

    def initialize(@data_dir : String, @file_name : String, @log : Log)
      @parameters = Hash(ParameterId?, T).new
      load!
    end

    forward_missing_to @parameters

    def create(data : JSON::Any, save = true)
      p = T.from_json(data)
      create(p, save)
    end

    def create(parameter : T, save = true)
      @parameters[parameter.name] = parameter
      save! if save
    end

    def delete(id) : T?
      if parameter = @parameters.delete id
        save!
        parameter
      end
    end

    def apply(parameter : Parameter? = nil)
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

    def each
      @parameters.each do |kv|
        yield kv
      end
    end

    def close
      save!
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
      tmpfile = File.join(@data_dir, "#{@file_name}.tmp")
      File.open(tmpfile, "w") { |f| self.to_pretty_json(f) }
      File.rename tmpfile, File.join(@data_dir, @file_name)
    end

    private def load!
      file = File.join(@data_dir, @file_name)
      if File.exists?(file)
        File.open(file, "r") do |f|
          Array(T).from_json(f).each { |p| create(p, save: false) }
        rescue JSON::ParseException
          @log.warn { "#{@file_name} is not vaild json" }
        end
      end
      @log.debug { "#{size} items loaded from #{@file_name}" }
    end
  end
end
