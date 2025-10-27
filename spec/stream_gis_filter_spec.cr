require "./spec_helper"
require "../src/lavinmq/amqp/stream/filters/gis.cr"

module GISFilterSpecHelper
  # Common test points
  NYC = {lat: 40.7128, lon: -74.0060}
  LA = {lat: 34.0522, lon: -118.2437}
  LONDON = {lat: 51.5074, lon: -0.1278}
  PARIS = {lat: 48.8566, lon: 2.3522}
  BROOKLYN = {lat: 40.6782, lon: -73.9442}
  MANHATTAN_CENTER = {lat: 40.7589, lon: -73.9851}

  # Triangle polygon points
  TRIANGLE_POINTS = [
    {lat: 40.0, lon: -74.0},
    {lat: 41.0, lon: -74.0},
    {lat: 40.5, lon: -73.0},
  ]

  def self.point_from_hash(h)
    LavinMQ::AMQP::GISFilter::Point.new(h[:lat], h[:lon])
  end

  def self.geo_headers(lat, lon)
    AMQ::Protocol::Table.new({"x-geo-lat" => lat, "x-geo-lon" => lon})
  end

  def self.make_triangle_polygon
    points = TRIANGLE_POINTS.map { |p| point_from_hash(p) }
    LavinMQ::AMQP::GISFilter::Polygon.new(points)
  end
end

describe LavinMQ::AMQP::GISFilter do
  describe "Point" do
    it "validates latitude bounds" do
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::GISFilter::Point.new(91.0, 0.0)
      end
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::GISFilter::Point.new(-91.0, 0.0)
      end
    end

    it "validates longitude bounds" do
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::GISFilter::Point.new(0.0, 181.0)
      end
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::GISFilter::Point.new(0.0, -181.0)
      end
    end

    it "accepts valid coordinates" do
      point = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      point.lat.should eq GISFilterSpecHelper::NYC[:lat]
      point.lon.should eq GISFilterSpecHelper::NYC[:lon]
    end
  end

  describe "BoundingBox" do
    it "validates min/max relationships" do
      expect_raises(ArgumentError, /min_lat must be <= max_lat/) do
        LavinMQ::AMQP::GISFilter::BoundingBox.new(50.0, 40.0, -75.0, -73.0)
      end
      expect_raises(ArgumentError, /min_lon must be <= max_lon/) do
        LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 50.0, -73.0, -75.0)
      end
    end

    it "validates coordinate bounds" do
      expect_raises(ArgumentError, /Latitude must be between -90 and 90/) do
        LavinMQ::AMQP::GISFilter::BoundingBox.new(-100.0, 50.0, -75.0, -73.0)
      end
      expect_raises(ArgumentError, /Longitude must be between -180 and 180/) do
        LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 50.0, -200.0, -73.0)
      end
    end

    it "contains point inside bounds" do
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::GISFilter::Point.new(40.5, -74.0)
      bbox.contains?(point).should be_true
    end

    it "excludes point outside bounds" do
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::GISFilter::Point.new(42.0, -74.0)
      bbox.contains?(point).should be_false
    end

    it "includes point on boundary" do
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      point = LavinMQ::AMQP::GISFilter::Point.new(40.0, -75.0)
      bbox.contains?(point).should be_true
    end
  end

  describe "Polygon" do
    it "requires at least 3 points" do
      expect_raises(ArgumentError, /Polygon must have at least 3 points/) do
        points = [
          LavinMQ::AMQP::GISFilter::Point.new(40.0, -74.0),
          LavinMQ::AMQP::GISFilter::Point.new(41.0, -74.0),
        ]
        LavinMQ::AMQP::GISFilter::Polygon.new(points)
      end
    end

    it "contains point inside triangle" do
      polygon = GISFilterSpecHelper.make_triangle_polygon
      test_point = LavinMQ::AMQP::GISFilter::Point.new(40.5, -73.8)
      polygon.contains?(test_point).should be_true
    end

    it "excludes point outside triangle" do
      polygon = GISFilterSpecHelper.make_triangle_polygon
      test_point = LavinMQ::AMQP::GISFilter::Point.new(42.0, -72.0)
      polygon.contains?(test_point).should be_false
    end

    it "handles complex polygon (Manhattan)" do
      # Simplified Manhattan bounding polygon
      points = [
        LavinMQ::AMQP::GISFilter::Point.new(40.7000, -74.0200),
        LavinMQ::AMQP::GISFilter::Point.new(40.8000, -74.0200),
        LavinMQ::AMQP::GISFilter::Point.new(40.8000, -73.9000),
        LavinMQ::AMQP::GISFilter::Point.new(40.7000, -73.9000),
      ]
      polygon = LavinMQ::AMQP::GISFilter::Polygon.new(points)

      # Times Square (inside Manhattan)
      times_square = LavinMQ::AMQP::GISFilter::Point.new(40.7580, -73.9855)
      polygon.contains?(times_square).should be_true

      # Brooklyn (outside Manhattan)
      brooklyn = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::BROOKLYN)
      polygon.contains?(brooklyn).should be_false
    end

    it "handles polygon with vertical edges without division by zero" do
      # Rectangle with vertical edges (same longitude for consecutive points)
      points = [
        LavinMQ::AMQP::GISFilter::Point.new(40.0, -75.0),
        LavinMQ::AMQP::GISFilter::Point.new(41.0, -75.0), # Vertical edge: lon=-75.0
        LavinMQ::AMQP::GISFilter::Point.new(41.0, -73.0),
        LavinMQ::AMQP::GISFilter::Point.new(40.0, -73.0), # Vertical edge: lon=-73.0
      ]
      polygon = LavinMQ::AMQP::GISFilter::Polygon.new(points)

      # Point inside the rectangle
      inside_point = LavinMQ::AMQP::GISFilter::Point.new(40.5, -74.0)
      polygon.contains?(inside_point).should be_true

      # Point outside to the left of the left vertical edge
      outside_left = LavinMQ::AMQP::GISFilter::Point.new(40.5, -76.0)
      polygon.contains?(outside_left).should be_false

      # Point outside to the right of the right vertical edge
      outside_right = LavinMQ::AMQP::GISFilter::Point.new(40.5, -72.0)
      polygon.contains?(outside_right).should be_false
    end
  end

  describe "Point#distance_to" do
    it "calculates distance between NYC and LA (approx 3935 km)" do
      nyc = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      la = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::LA)
      distance = nyc.distance_to(la)
      distance.should be_close(3935.0, 50.0) # Within 50km tolerance
    end

    it "calculates distance between London and Paris (approx 344 km)" do
      london = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::LONDON)
      paris = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::PARIS)
      distance = london.distance_to(paris)
      distance.should be_close(344.0, 10.0) # Within 10km tolerance
    end

    it "returns zero for same point" do
      nyc = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      distance = nyc.distance_to(nyc)
      distance.should be_close(0.0, 0.001)
    end
  end

  describe "Point.from_headers" do
    it "extracts point from headers with Float64 values" do
      headers = GISFilterSpecHelper.geo_headers(40.7128, -74.0060)
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.7128
      point.not_nil!.lon.should eq -74.0060
    end

    it "extracts point from headers with Int values" do
      headers = GISFilterSpecHelper.geo_headers(40, -74)
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.0
      point.not_nil!.lon.should eq -74.0
    end

    it "extracts point from headers with String values" do
      headers = GISFilterSpecHelper.geo_headers("40.7128", "-74.0060")
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should_not be_nil
      point.not_nil!.lat.should eq 40.7128
      point.not_nil!.lon.should eq -74.0060
    end

    it "returns nil for missing headers" do
      headers = AMQ::Protocol::Table.new({"foo" => "bar"})
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should be_nil
    end

    it "returns nil for nil headers" do
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(nil)
      point.should be_nil
    end

    it "returns nil for invalid lat/lon values" do
      headers = GISFilterSpecHelper.geo_headers("invalid", "also-invalid")
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should be_nil
    end

    it "returns nil for out-of-bounds coordinates" do
      headers = GISFilterSpecHelper.geo_headers(100.0, -74.0)
      point = LavinMQ::AMQP::GISFilter::Point.from_headers(headers)
      point.should be_nil
    end
  end

  describe "parse_radius_filter" do
    it "parses valid radius filter" do
      table = AMQ::Protocol::Table.new({
        "lat"       => GISFilterSpecHelper::NYC[:lat],
        "lon"       => GISFilterSpecHelper::NYC[:lon],
        "radius_km" => 10.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter(table)
      result.should_not be_nil
      filter = result.not_nil!
      filter.center.lat.should eq GISFilterSpecHelper::NYC[:lat]
      filter.center.lon.should eq GISFilterSpecHelper::NYC[:lon]
      filter.radius_km.should eq 10.0
    end

    it "returns nil for missing fields" do
      table = AMQ::Protocol::Table.new({
        "lat" => GISFilterSpecHelper::NYC[:lat],
        "lon" => GISFilterSpecHelper::NYC[:lon],
      })
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter(table)
      result.should be_nil
    end

    it "returns nil for zero radius" do
      table = AMQ::Protocol::Table.new({
        "lat"       => GISFilterSpecHelper::NYC[:lat],
        "lon"       => GISFilterSpecHelper::NYC[:lon],
        "radius_km" => 0.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter(table)
      result.should be_nil
    end

    it "returns nil for negative radius" do
      table = AMQ::Protocol::Table.new({
        "lat"       => GISFilterSpecHelper::NYC[:lat],
        "lon"       => GISFilterSpecHelper::NYC[:lon],
        "radius_km" => -5.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter(table)
      result.should be_nil
    end

    it "returns nil for non-table input" do
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter("not a table")
      result.should be_nil
    end

    it "returns nil for invalid numeric values" do
      table = AMQ::Protocol::Table.new({
        "lat"       => "not-a-number",
        "lon"       => GISFilterSpecHelper::NYC[:lon],
        "radius_km" => 10.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_radius_filter(table)
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
      result = LavinMQ::AMQP::GISFilter.parse_bbox_filter(table)
      result.should_not be_nil
      filter = result.not_nil!
      filter.bbox.min_lat.should eq 40.0
      filter.bbox.max_lat.should eq 41.0
      filter.bbox.min_lon.should eq -75.0
      filter.bbox.max_lon.should eq -73.0
    end

    it "returns nil for missing fields" do
      table = AMQ::Protocol::Table.new({"min_lat" => 40.0, "max_lat" => 41.0})
      result = LavinMQ::AMQP::GISFilter.parse_bbox_filter(table)
      result.should be_nil
    end

    it "returns nil for invalid bounds" do
      table = AMQ::Protocol::Table.new({
        "min_lat" => 50.0,
        "max_lat" => 40.0, # Invalid: min > max
        "min_lon" => -75.0,
        "max_lon" => -73.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_bbox_filter(table)
      result.should be_nil
    end

    it "returns nil for non-table input" do
      result = LavinMQ::AMQP::GISFilter.parse_bbox_filter("not a table")
      result.should be_nil
    end

    it "returns nil for invalid numeric values" do
      table = AMQ::Protocol::Table.new({
        "min_lat" => "invalid",
        "max_lat" => 41.0,
        "min_lon" => -75.0,
        "max_lon" => -73.0,
      })
      result = LavinMQ::AMQP::GISFilter.parse_bbox_filter(table)
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
      result = LavinMQ::AMQP::GISFilter.parse_polygon_filter(table)
      result.should_not be_nil
      filter = result.not_nil!
      filter.polygon.points.size.should eq 3
    end

    it "returns nil for fewer than 3 points" do
      points_array = [
        [40.0, -74.0] of AMQ::Protocol::Field,
        [41.0, -74.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::GISFilter.parse_polygon_filter(table)
      result.should be_nil
    end

    it "returns nil for invalid point format" do
      points_array = [
        [40.0] of AMQ::Protocol::Field, # Only 1 coordinate
        [41.0, -74.0] of AMQ::Protocol::Field,
        [40.5, -73.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::GISFilter.parse_polygon_filter(table)
      result.should be_nil
    end

    it "returns nil for non-table input" do
      result = LavinMQ::AMQP::GISFilter.parse_polygon_filter("not a table")
      result.should be_nil
    end

    it "returns nil for invalid numeric values" do
      points_array = [
        ["invalid", -74.0] of AMQ::Protocol::Field,
        [41.0, -74.0] of AMQ::Protocol::Field,
        [40.5, -73.0] of AMQ::Protocol::Field,
      ] of AMQ::Protocol::Field
      table = AMQ::Protocol::Table.new({"points" => points_array})
      result = LavinMQ::AMQP::GISFilter.parse_polygon_filter(table)
      result.should be_nil
    end
  end

  describe "RadiusFilter#match?" do
    it "matches point within radius" do
      headers = GISFilterSpecHelper.geo_headers(GISFilterSpecHelper::NYC[:lat], GISFilterSpecHelper::NYC[:lon])
      center = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      filter = LavinMQ::AMQP::GISFilter::RadiusFilter.new(center, 10.0)
      filter.match?(headers).should be_true
    end

    it "rejects point outside radius" do
      headers = GISFilterSpecHelper.geo_headers(GISFilterSpecHelper::BROOKLYN[:lat], GISFilterSpecHelper::BROOKLYN[:lon])
      center = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::MANHATTAN_CENTER)
      filter = LavinMQ::AMQP::GISFilter::RadiusFilter.new(center, 1.0)
      filter.match?(headers).should be_false
    end

    it "matches point on radius boundary" do
      headers = GISFilterSpecHelper.geo_headers(40.8028, -74.0060)
      center = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      filter = LavinMQ::AMQP::GISFilter::RadiusFilter.new(center, 10.1)
      filter.match?(headers).should be_true
    end

    it "returns false for headers missing geo coordinates" do
      headers = AMQ::Protocol::Table.new({"foo" => "bar"})
      center = GISFilterSpecHelper.point_from_hash(GISFilterSpecHelper::NYC)
      filter = LavinMQ::AMQP::GISFilter::RadiusFilter.new(center, 10.0)
      filter.match?(headers).should be_false
    end
  end

  describe "BoundingBoxFilter#match?" do
    it "matches point inside bounding box" do
      headers = GISFilterSpecHelper.geo_headers(40.5, -74.0)
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      filter = LavinMQ::AMQP::GISFilter::BoundingBoxFilter.new(bbox)
      filter.match?(headers).should be_true
    end

    it "rejects point outside bounding box" do
      headers = GISFilterSpecHelper.geo_headers(42.0, -74.0)
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      filter = LavinMQ::AMQP::GISFilter::BoundingBoxFilter.new(bbox)
      filter.match?(headers).should be_false
    end

    it "returns false for headers missing geo coordinates" do
      headers = AMQ::Protocol::Table.new({"foo" => "bar"})
      bbox = LavinMQ::AMQP::GISFilter::BoundingBox.new(40.0, 41.0, -75.0, -73.0)
      filter = LavinMQ::AMQP::GISFilter::BoundingBoxFilter.new(bbox)
      filter.match?(headers).should be_false
    end
  end

  describe "PolygonFilter#match?" do
    it "matches point inside polygon" do
      headers = GISFilterSpecHelper.geo_headers(40.5, -73.8)
      polygon = GISFilterSpecHelper.make_triangle_polygon
      filter = LavinMQ::AMQP::GISFilter::PolygonFilter.new(polygon)
      filter.match?(headers).should be_true
    end

    it "rejects point outside polygon" do
      headers = GISFilterSpecHelper.geo_headers(42.0, -72.0)
      polygon = GISFilterSpecHelper.make_triangle_polygon
      filter = LavinMQ::AMQP::GISFilter::PolygonFilter.new(polygon)
      filter.match?(headers).should be_false
    end

    it "returns false for headers missing geo coordinates" do
      headers = AMQ::Protocol::Table.new({"foo" => "bar"})
      polygon = GISFilterSpecHelper.make_triangle_polygon
      filter = LavinMQ::AMQP::GISFilter::PolygonFilter.new(polygon)
      filter.match?(headers).should be_false
    end
  end
end
