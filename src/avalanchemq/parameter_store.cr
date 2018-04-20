require "json"
require "./parameter"

module AvalancheMQ
  class ParameterStore(T)
    def initialize(@data_dir : String, @file_name : String, @log : Logger)
      @parameters = Hash(ParameterId?, T).new
      load!
    end

    def [](id)
      @parameters[id]
    end

    def []?(id)
      @parameters[id]?
    end

    def []=(id : ParameterId?, p : T)
      @parameters[id] = p
      save!
    end

    def as_h
      @parameters
    end

    def size
      @parameters.size
    end

    def create(id, data)
      self[id] = T.from_json(data)
    end

    def delete(id) : T?
      if parameter = @parameters.delete id
        save!
        parameter
      end
    end

    def close
      save!
    end

    def to_json(json : JSON::Builder)
      @parameters.values.to_json(json)
    end

    def save!
      @log.debug "Saving #{@file_name}"
      tmpfile = File.join(@data_dir, "#{@file_name}.tmp")
      File.open(tmpfile, "w") { |f| self.to_json(f) }
      File.rename tmpfile, File.join(@data_dir, @file_name)
    end

    private def load!
      file = File.join(@data_dir, @file_name)
      if File.exists?(file)
        File.open(file, "r") do |f|
          data = JSON.parse(f)
          data.each do |p|
            parameter = T.from_json(p)
            @parameters[parameter.name] = parameter
          end
        end
      end
      @log.debug("#{@parameters.size} items loaded from #{@file_name}")
    end
  end
end
