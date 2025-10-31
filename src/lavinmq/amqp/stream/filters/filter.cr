module LavinMQ::AMQP
  module StreamFilter
    abstract def match?(headers : AMQP::Table) : Bool

    # Parse all filters from consumer arguments
    def self.from_arguments(arguments : AMQ::Protocol::Table) : Array(StreamFilter)
      filters = Array(StreamFilter).new

      if stream_filter = arguments["x-stream-filter"]?
        parse_stream_filter(stream_filter, filters)
      end

      parse_gis_filters(arguments, filters)

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
          if k.to_s == "x-stream-filter"
            v.to_s.split(',').each do |f|
              filters << XStreamFilter.new(f.strip)
            end
          else
            filters << KVFilter.new(k.to_s, v.to_s)
          end
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

    private def self.parse_gis_filters(arguments : AMQ::Protocol::Table, filters)
      if radius_arg = validate_filter_table(arguments, "x-geo-within-radius")
        begin
          filters << GISFilter.parse_radius_filter(radius_arg)
        rescue ex : ArgumentError
          raise LavinMQ::Error::PreconditionFailed.new("x-geo-within-radius: #{ex.message}", cause: ex)
        end
      end

      if bbox_arg = validate_filter_table(arguments, "x-geo-bbox")
        begin
          filters << GISFilter.parse_bbox_filter(bbox_arg)
        rescue ex : ArgumentError
          raise LavinMQ::Error::PreconditionFailed.new("x-geo-bbox: #{ex.message}", cause: ex)
        end
      end

      if polygon_arg = validate_filter_table(arguments, "x-geo-polygon")
        begin
          filters << GISFilter.parse_polygon_filter(polygon_arg)
        rescue ex : ArgumentError
          raise LavinMQ::Error::PreconditionFailed.new("x-geo-polygon: #{ex.message}", cause: ex)
        end
      end
    end

    private def self.validate_filter_table(field : AMQ::Protocol::Table, key : String) : AMQ::Protocol::Table | Nil
      if value = field["#{key}"]?
        unless value.is_a?(AMQ::Protocol::Table)
          raise LavinMQ::Error::PreconditionFailed.new("Expected AMQ::Protocol::Table for #{key}")
        end
        return value
      end
      nil
    end
  end
end
