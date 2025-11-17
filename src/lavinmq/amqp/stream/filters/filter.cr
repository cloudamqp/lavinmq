module LavinMQ::AMQP
  module StreamFilter
    abstract def match?(headers : AMQP::Table) : Bool

    # Parse all filters from consumer arguments
    def self.from_arguments(arguments : AMQ::Protocol::Table) : Array(StreamFilter)
      filters = Array(StreamFilter).new

      if stream_filter = arguments["x-stream-filter"]?
        parse_stream_filter(stream_filter, filters)
      end

      filters
    end

    private def self.parse_stream_filter(arg, filters)
      case arg
      when String
        arg.split(',').each do |f|
          filters << XStreamFilter.new(f.strip)
        end
      when AMQ::Protocol::Table
        arg.each do |k, v|
          parse_table_filter(k.to_s, v, filters)
        end
      when Array
        arg.each do |f|
          parse_stream_filter(f, filters)
        end
      when Nil
        # noop
      else
        raise LavinMQ::Error::PreconditionFailed.new("x-stream-filter must be a string, table, or array")
      end
    end

    private def self.parse_table_filter(key : String, value, filters)
      case key
      when "x-stream-filter"
        if value.is_a?(String)
          value.split(',').each do |v|
            filters << XStreamFilter.new(v.strip)
          end
        end
      when "geo-within-radius"
        table_value = validate_table_value(value, key)
        filters << GISFilter.parse_radius_filter(table_value)
      when "geo-bbox"
        table_value = validate_table_value(value, key)
        filters << GISFilter.parse_bbox_filter(table_value)
      when "geo-polygon"
        table_value = validate_table_value(value, key)
        filters << GISFilter.parse_polygon_filter(table_value)
      else
        filters << KVFilter.new(key, value.to_s)
      end
    rescue e : ArgumentError
      raise LavinMQ::Error::PreconditionFailed.new("Invalid value for filter #{key}: #{e.message}")
    end

    private def self.validate_table_value(value, key : String) : AMQ::Protocol::Table
      unless value.is_a?(AMQ::Protocol::Table)
        raise LavinMQ::Error::PreconditionFailed.new("Expected AMQ::Protocol::Table for #{key}")
      end
      value
    end
  end
end
