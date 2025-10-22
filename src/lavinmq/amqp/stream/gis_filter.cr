require "amq-protocol"

module LavinMQ
  module AMQP
    # GIS filtering module for geographical data in stream queues.
    # Provides PostGIS-like spatial operations for filtering messages based on coordinates.
    module StreamGISFilter
        EARTH_RADIUS_KM = 6371.0

        # Represents a geographical point with latitude and longitude in decimal degrees
        struct Point
          getter lat : Float64
          getter lon : Float64

          def initialize(@lat : Float64, @lon : Float64)
            validate!
          end

          private def validate!
            raise ArgumentError.new("Latitude must be between -90 and 90") unless @lat >= -90 && @lat <= 90
            raise ArgumentError.new("Longitude must be between -180 and 180") unless @lon >= -180 && @lon <= 180
          end

          def to_s(io : IO)
            io << "Point(lat=" << @lat << ", lon=" << @lon << ")"
          end
        end

        # Represents a bounding box defined by min/max latitude and longitude
        struct BoundingBox
          getter min_lat : Float64
          getter max_lat : Float64
          getter min_lon : Float64
          getter max_lon : Float64

          def initialize(@min_lat : Float64, @max_lat : Float64, @min_lon : Float64, @max_lon : Float64)
            validate!
          end

          private def validate!
            raise ArgumentError.new("min_lat must be <= max_lat") unless @min_lat <= @max_lat
            raise ArgumentError.new("min_lon must be <= max_lon") unless @min_lon <= @max_lon
            raise ArgumentError.new("Latitude must be between -90 and 90") unless @min_lat >= -90 && @max_lat <= 90
            raise ArgumentError.new("Longitude must be between -180 and 180") unless @min_lon >= -180 && @max_lon <= 180
          end

          def contains?(point : Point) : Bool
            point.lat >= @min_lat && point.lat <= @max_lat &&
              point.lon >= @min_lon && point.lon <= @max_lon
          end

          def to_s(io : IO)
            io << "BoundingBox(lat=" << @min_lat << ".." << @max_lat
            io << ", lon=" << @min_lon << ".." << @max_lon << ")"
          end
        end

        # Represents a polygon as an array of points
        struct Polygon
          getter points : Array(Point)

          def initialize(@points : Array(Point))
            raise ArgumentError.new("Polygon must have at least 3 points") if @points.size < 3
          end

          # Point-in-polygon test using ray casting algorithm
          # https://en.wikipedia.org/wiki/Point_in_polygon
          def contains?(point : Point) : Bool
            inside = false
            j = @points.size - 1

            @points.size.times do |i|
              pi = @points[i]
              pj = @points[j]

              if ((pi.lon > point.lon) != (pj.lon > point.lon)) &&
                 (point.lat < (pj.lat - pi.lat) * (point.lon - pi.lon) / (pj.lon - pi.lon) + pi.lat)
                inside = !inside
              end

              j = i
            end

            inside
          end

          def to_s(io : IO)
            io << "Polygon(" << @points.size << " points)"
          end
        end

        # Calculate distance between two points using Haversine formula
        # Returns distance in kilometers
        def self.haversine_distance(p1 : Point, p2 : Point) : Float64
          lat1_rad = p1.lat * Math::PI / 180.0
          lat2_rad = p2.lat * Math::PI / 180.0
          delta_lat = (p2.lat - p1.lat) * Math::PI / 180.0
          delta_lon = (p2.lon - p1.lon) * Math::PI / 180.0

          a = Math.sin(delta_lat / 2) ** 2 +
              Math.cos(lat1_rad) * Math.cos(lat2_rad) *
              Math.sin(delta_lon / 2) ** 2

          c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

          EARTH_RADIUS_KM * c
        end

        # Extract geographical point from message headers
        # Returns nil if headers don't contain valid lat/lon
        def self.extract_point(headers : ::AMQ::Protocol::Table?) : Point?
          return nil unless headers

          lat = headers["x-geo-lat"]?
          lon = headers["x-geo-lon"]?

          return nil unless lat && lon

          # Convert to Float64 if needed
          lat_f = case lat
                  when Int32, Int64, Float32, Float64
                    lat.to_f64
                  when String
                    lat.to_f64?
                  else
                    nil
                  end

          lon_f = case lon
                  when Int32, Int64, Float32, Float64
                    lon.to_f64
                  when String
                    lon.to_f64?
                  else
                    nil
                  end

          return nil unless lat_f && lon_f

          Point.new(lat_f, lon_f)
        rescue ArgumentError
          nil
        end

        # Parse radius filter from consumer arguments
        # Expected format: {"lat": Float64, "lon": Float64, "radius_km": Float64}
        def self.parse_radius_filter(value : ::AMQ::Protocol::Field) : Tuple(Point, Float64)?
          return nil unless value.is_a?(::AMQ::Protocol::Table)

          lat = value["lat"]?
          lon = value["lon"]?
          radius = value["radius_km"]?

          return nil unless lat && lon && radius

          lat_f = to_float64?(lat)
          lon_f = to_float64?(lon)
          radius_f = to_float64?(radius)

          return nil unless lat_f && lon_f && radius_f
          return nil if radius_f <= 0

          {Point.new(lat_f, lon_f), radius_f}
        rescue ArgumentError
          nil
        end

        # Parse bounding box filter from consumer arguments
        # Expected format: {"min_lat": Float64, "max_lat": Float64, "min_lon": Float64, "max_lon": Float64}
        def self.parse_bbox_filter(value : ::AMQ::Protocol::Field) : BoundingBox?
          return nil unless value.is_a?(::AMQ::Protocol::Table)

          min_lat = to_float64?(value["min_lat"]?)
          max_lat = to_float64?(value["max_lat"]?)
          min_lon = to_float64?(value["min_lon"]?)
          max_lon = to_float64?(value["max_lon"]?)

          return nil unless min_lat && max_lat && min_lon && max_lon

          BoundingBox.new(min_lat, max_lat, min_lon, max_lon)
        rescue ArgumentError
          nil
        end

        # Parse polygon filter from consumer arguments
        # Expected format: {"points": [[lat1, lon1], [lat2, lon2], ...]}
        def self.parse_polygon_filter(value : ::AMQ::Protocol::Field) : Polygon?
          return nil unless value.is_a?(::AMQ::Protocol::Table)

          points_field = value["points"]?
          return nil unless points_field.is_a?(Array)

          points = [] of Point

          points_field.each do |point_field|
            next unless point_field.is_a?(Array)
            next unless point_field.size == 2

            lat = to_float64?(point_field[0])
            lon = to_float64?(point_field[1])

            return nil unless lat && lon

            points << Point.new(lat, lon)
          end

          return nil if points.size < 3

          Polygon.new(points)
        rescue ArgumentError
          nil
        end

        # Helper to convert AMQ::Protocol::Field to Float64
        private def self.to_float64?(value : ::AMQ::Protocol::Field?) : Float64?
          return nil unless value

          case value
          when Int32, Int64, Float32, Float64
            value.to_f64
          when String
            value.to_f64?
          else
            nil
          end
        end

        # Check if message location matches radius filter
        def self.match_radius?(msg_headers : ::AMQ::Protocol::Table?, center : Point, radius_km : Float64) : Bool
          msg_point = extract_point(msg_headers)
          return false unless msg_point

          haversine_distance(msg_point, center) <= radius_km
        end

        # Check if message location matches bounding box filter
        def self.match_bbox?(msg_headers : ::AMQ::Protocol::Table?, bbox : BoundingBox) : Bool
          msg_point = extract_point(msg_headers)
          return false unless msg_point

          bbox.contains?(msg_point)
        end

        # Check if message location matches polygon filter
        def self.match_polygon?(msg_headers : ::AMQ::Protocol::Table?, polygon : Polygon) : Bool
          msg_point = extract_point(msg_headers)
          return false unless msg_point

          polygon.contains?(msg_point)
        end
    end
  end
end
