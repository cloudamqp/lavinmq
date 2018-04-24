require "json"
require "./parameter"

module AvalancheMQ
  class ParameterStore(T)
    include Enumerable(T)

    def initialize(@data_dir : String, @file_name : String, @log : Logger)
      @parameters = Hash(ParameterId?, T).new
      load!
    end

    def each
      @parameters.values.each { |e| yield e }
    end

    def [](id)
      @parameters[id]
    end

    def []?(id)
      @parameters[id]?
    end

    def values
      @parameters.values
    end

    def size
      @parameters.size
    end

    def empty?
      @parameters.empty?
    end

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
          Array(T).from_json(f).each { |p| create(p, save: false) }
        end
      end
      @log.debug("#{@parameters.size} items loaded from #{@file_name}")
    end
  end
end
