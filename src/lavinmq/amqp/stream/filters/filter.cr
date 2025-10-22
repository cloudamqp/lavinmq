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
      if radius_arg = arguments["x-geo-within-radius"]?
        gis_radius_filter = GISFilter.parse_radius_filter(radius_arg)
        if gis_radius_filter.nil?
          raise LavinMQ::Error::PreconditionFailed.new(
            "x-geo-within-radius must be a table with 'lat', 'lon', and 'radius_km' (positive number)"
          )
        end
        filters << gis_radius_filter
      end

      if bbox_arg = arguments["x-geo-bbox"]?
        gis_bbox_filter = GISFilter.parse_bbox_filter(bbox_arg)
        if gis_bbox_filter.nil?
          raise LavinMQ::Error::PreconditionFailed.new(
            "x-geo-bbox must be a table with 'min_lat', 'max_lat', 'min_lon', and 'max_lon'"
          )
        end
        filters << gis_bbox_filter
      end

      if polygon_arg = arguments["x-geo-polygon"]?
        gis_polygon_filter = GISFilter.parse_polygon_filter(polygon_arg)
        if gis_polygon_filter.nil?
          raise LavinMQ::Error::PreconditionFailed.new(
            "x-geo-polygon must be a table with 'points' array of [lat, lon] pairs (at least 3 points)"
          )
        end
        filters << gis_polygon_filter
      end
    end
  end
end
