require "./spec_helper"
require "../src/lavinmq/amqp/stream/gis_filter"

describe LavinMQ::AMQP::StreamGISFilter do
  describe "Point" do
    it "validates latitude bounds" do
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::StreamGISFilter::Point.new(91.0, 0.0)
      end
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::StreamGISFilter::Point.new(-91.0, 0.0)
      end
    end

    it "validates longitude bounds" do
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::StreamGISFilter::Point.new(0.0, 181.0)
      end
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::StreamGISFilter::Point.new(0.0, -181.0)
      end
    end

    it "accepts valid coordinates" do
      point = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7128, -74.0060)
      point.lat.should eq 40.7128
      point.lon.should eq -74.0060
    end
  end

  describe "BoundingBox" do
    it "validates min/max relationships" do
      expect_raises(ArgumentError, /min_lat must be <= max_lat/) do
        LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(50.0, 40.0, -75.0, -73.0)
      end
      expect_raises(ArgumentError, /min_lon must be <= max_lon/) do
        LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 50.0, -73.0, -75.0)
      end
    end

    it "validates coordinate bounds" do
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(-100.0, 50.0, -75.0, -73.0)
      end
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 50.0, -200.0, -73.0)
      end
    end

    it "contains point inside bounds" do
      bbox = LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -74.0)
      bbox.contains?(point).should be_true
    end

    it "excludes point outside bounds" do
      bbox = LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::StreamGISFilter::Point.new(42.0, -74.0)
      bbox.contains?(point).should be_false
    end

    it "includes point on boundary" do
      bbox = LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -75.0)
      bbox.contains?(point).should be_true
    end
  end

  describe "Polygon" do
    it "requires at least 3 points" do
      expect_raises(ArgumentError, /Polygon must have at least 3 points/) do
        points = [
          LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -74.0),
          LavinMQ::AMQP::StreamGISFilter::Point.new(41.0, -74.0),
        ]
        LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)
      end
    end

    it "contains point inside triangle" do
      points = [
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(41.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -73.0),
      ]
      polygon = LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)
      test_point = LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -73.8)
      polygon.contains?(test_point).should be_true
    end

    it "excludes point outside triangle" do
      points = [
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(41.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -73.0),
      ]
      polygon = LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)
      test_point = LavinMQ::AMQP::StreamGISFilter::Point.new(42.0, -72.0)
      polygon.contains?(test_point).should be_false
    end

    it "handles complex polygon (Manhattan)" do
      # Simplified Manhattan bounding polygon
      points = [
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.7000, -74.0200),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.8000, -74.0200),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.8000, -73.9000),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.7000, -73.9000),
      ]
      polygon = LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)

      # Times Square (inside Manhattan)
      times_square = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7580, -73.9855)
      polygon.contains?(times_square).should be_true

      # Brooklyn (outside Manhattan)
      brooklyn = LavinMQ::AMQP::StreamGISFilter::Point.new(40.6782, -73.9442)
      polygon.contains?(brooklyn).should be_false
    end
  end

  describe "haversine_distance" do
    it "calculates distance between NYC and LA (approx 3935 km)" do
      nyc = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7128, -74.0060)
      la = LavinMQ::AMQP::StreamGISFilter::Point.new(34.0522, -118.2437)
      distance = LavinMQ::AMQP::StreamGISFilter.haversine_distance(nyc, la)
      distance.should be_close(3935.0, 50.0) # Within 50km tolerance
    end

    it "calculates distance between London and Paris (approx 344 km)" do
      london = LavinMQ::AMQP::StreamGISFilter::Point.new(51.5074, -0.1278)
      paris = LavinMQ::AMQP::StreamGISFilter::Point.new(48.8566, 2.3522)
      distance = LavinMQ::AMQP::StreamGISFilter.haversine_distance(london, paris)
      distance.should be_close(344.0, 10.0) # Within 10km tolerance
    end

    it "returns zero for same point" do
      nyc = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7128, -74.0060)
      distance = LavinMQ::AMQP::StreamGISFilter.haversine_distance(nyc, nyc)
      distance.should be_close(0.0, 0.001)
    end
  end

  describe "extract_point" do
    it "extracts point from headers with Float64 values" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.7128, "x-geo-lon" => -74.0060})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.7128
      point.not_nil!.lon.should eq -74.0060
    end

    it "extracts point from headers with Int values" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40, "x-geo-lon" => -74})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.0
      point.not_nil!.lon.should eq -74.0
    end

    it "extracts point from headers with String values" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => "40.7128", "x-geo-lon" => "-74.0060"})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.7128
      point.not_nil!.lon.should eq -74.0060
    end

    it "returns nil for missing headers" do
      headers = AMQ::Protocol::Table.new({"foo" => "bar"})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should be_nil
    end

    it "returns nil for nil headers" do
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(nil)
      point.should be_nil
    end

    it "returns nil for invalid lat/lon values" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => "invalid", "x-geo-lon" => "also-invalid"})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should be_nil
    end

    it "returns nil for out-of-bounds coordinates" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 100.0, "x-geo-lon" => -74.0})
      point = LavinMQ::AMQP::StreamGISFilter.extract_point(headers)
      point.should be_nil
    end
  end

  describe "parse_radius_filter" do
    it "parses valid radius filter" do
      table = AMQ::Protocol::Table.new({
        "lat"       => 40.7128,
        "lon"       => -74.0060,
        "radius_km" => 10.0,
      })
      result = LavinMQ::AMQP::StreamGISFilter.parse_radius_filter(table)
      result.should_not be_nil
      center, radius = result.not_nil!
      center.lat.should eq 40.7128
      center.lon.should eq -74.0060
      radius.should eq 10.0
    end

    it "returns nil for missing fields" do
      table = AMQ::Protocol::Table.new({"lat" => 40.7128, "lon" => -74.0060})
      result = LavinMQ::AMQP::StreamGISFilter.parse_radius_filter(table)
      result.should be_nil
    end

    it "returns nil for zero or negative radius" do
      table = AMQ::Protocol::Table.new({
        "lat"       => 40.7128,
        "lon"       => -74.0060,
        "radius_km" => 0.0,
      })
      result = LavinMQ::AMQP::StreamGISFilter.parse_radius_filter(table)
      result.should be_nil
    end

    it "returns nil for non-table input" do
      result = LavinMQ::AMQP::StreamGISFilter.parse_radius_filter("not a table")
      result.should be_nil
    end
  end

  describe "parse_bbox_filter" do
    it "parses valid bounding box" do
      table = AMQ::Protocol::Table.new({
        "min_lat" => 40.0,
        "max_lat" => 41.0,
        "min_lon" => -75.0,
        "max_lon" => -73.0,
      })
      result = LavinMQ::AMQP::StreamGISFilter.parse_bbox_filter(table)
      result.should_not be_nil
      bbox = result.not_nil!
      bbox.min_lat.should eq 40.0
      bbox.max_lat.should eq 41.0
      bbox.min_lon.should eq -75.0
      bbox.max_lon.should eq -73.0
    end

    it "returns nil for missing fields" do
      table = AMQ::Protocol::Table.new({"min_lat" => 40.0, "max_lat" => 41.0})
      result = LavinMQ::AMQP::StreamGISFilter.parse_bbox_filter(table)
      result.should be_nil
    end

    it "returns nil for invalid bounds" do
      table = AMQ::Protocol::Table.new({
        "min_lat" => 50.0,
        "max_lat" => 40.0, # Invalid: min > max
        "min_lon" => -75.0,
        "max_lon" => -73.0,
      })
      result = LavinMQ::AMQP::StreamGISFilter.parse_bbox_filter(table)
      result.should be_nil
    end
  end

  describe "parse_polygon_filter" do
    it "parses valid polygon" do
      points_array = [
        [40.0, -74.0] of AMQ::Protocol::Field,
        [41.0, -74.0] of AMQ::Protocol::Field,
        [40.5, -73.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::StreamGISFilter.parse_polygon_filter(table)
      result.should_not be_nil
      polygon = result.not_nil!
      polygon.points.size.should eq 3
    end

    it "returns nil for fewer than 3 points" do
      points_array = [
        [40.0, -74.0] of AMQ::Protocol::Field,
        [41.0, -74.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::StreamGISFilter.parse_polygon_filter(table)
      result.should be_nil
    end

    it "returns nil for invalid point format" do
      points_array = [
        [40.0] of AMQ::Protocol::Field,        # Only 1 coordinate
        [41.0, -74.0] of AMQ::Protocol::Field,
        [40.5, -73.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::StreamGISFilter.parse_polygon_filter(table)
      result.should be_nil
    end
  end

  describe "match_radius?" do
    it "matches point within radius" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.7128, "x-geo-lon" => -74.0060})
      center = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7128, -74.0060)
      LavinMQ::AMQP::StreamGISFilter.match_radius?(headers, center, 10.0).should be_true
    end

    it "rejects point outside radius" do
      # Brooklyn (far from center)
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.6782, "x-geo-lon" => -73.9442})
      # Manhattan center
      center = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7589, -73.9851)
      LavinMQ::AMQP::StreamGISFilter.match_radius?(headers, center, 1.0).should be_false
    end

    it "matches point on radius boundary" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.8028, "x-geo-lon" => -74.0060})
      center = LavinMQ::AMQP::StreamGISFilter::Point.new(40.7128, -74.0060)
      # Distance is approximately 10km
      LavinMQ::AMQP::StreamGISFilter.match_radius?(headers, center, 10.1).should be_true
    end
  end

  describe "match_bbox?" do
    it "matches point inside bounding box" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.5, "x-geo-lon" => -74.0})
      bbox = LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      LavinMQ::AMQP::StreamGISFilter.match_bbox?(headers, bbox).should be_true
    end

    it "rejects point outside bounding box" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 42.0, "x-geo-lon" => -74.0})
      bbox = LavinMQ::AMQP::StreamGISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      LavinMQ::AMQP::StreamGISFilter.match_bbox?(headers, bbox).should be_false
    end
  end

  describe "match_polygon?" do
    it "matches point inside polygon" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 40.5, "x-geo-lon" => -73.8})
      points = [
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(41.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -73.0),
      ]
      polygon = LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)
      LavinMQ::AMQP::StreamGISFilter.match_polygon?(headers, polygon).should be_true
    end

    it "rejects point outside polygon" do
      headers = AMQ::Protocol::Table.new({"x-geo-lat" => 42.0, "x-geo-lon" => -72.0})
      points = [
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(41.0, -74.0),
        LavinMQ::AMQP::StreamGISFilter::Point.new(40.5, -73.0),
      ]
      polygon = LavinMQ::AMQP::StreamGISFilter::Polygon.new(points)
      LavinMQ::AMQP::StreamGISFilter.match_polygon?(headers, polygon).should be_false
    end
  end
end
