require "./spec_helper"

describe LavinMQ::AMQP::Stream do
  describe "GIS filtering" do
    it "should filter messages by radius" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Publish messages at different locations around NYC (Times Square)
          times_square_lat = 40.7580
          times_square_lon = -73.9855

          # Message 1: Times Square (center point) - should match
          q.publish("times_square", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => times_square_lat,
              "x-geo-lon" => times_square_lon,
            })
          ))

          # Message 2: Empire State Building (1km away) - should match
          q.publish("empire_state", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7484,
              "x-geo-lon" => -73.9857,
            })
          ))

          # Message 3: Brooklyn (9km away) - should NOT match
          q.publish("brooklyn", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.6782,
              "x-geo-lon" => -73.9442,
            })
          ))

          # Subscribe with 5km radius filter around Times Square
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset"     => "first",
            "x-geo-within-radius" => {
              "lat"       => times_square_lat,
              "lon"       => times_square_lon,
              "radius_km" => 5.0,
            },
          })) do |msg|
            msgs.send msg
          end

          # Should receive the two nearby messages
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "times_square"
          msg1.ack

          msg2 = msgs.receive
          msg2.body_io.to_s.should eq "empire_state"
          msg2.ack

          # Should NOT receive Brooklyn message
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should filter messages by bounding box" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-bbox", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Define Manhattan bounding box (simplified)
          manhattan_bbox = {
            "min_lat" => 40.7000,
            "max_lat" => 40.8000,
            "min_lon" => -74.0200,
            "max_lon" => -73.9000,
          }

          # Message 1: Times Square (inside Manhattan) - should match
          q.publish("times_square", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7580,
              "x-geo-lon" => -73.9855,
            })
          ))

          # Message 2: Central Park (inside Manhattan) - should match
          q.publish("central_park", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7829,
              "x-geo-lon" => -73.9654,
            })
          ))

          # Message 3: Brooklyn (outside Manhattan) - should NOT match
          q.publish("brooklyn", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.6782,
              "x-geo-lon" => -73.9442,
            })
          ))

          # Subscribe with bounding box filter
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset" => "first",
            "x-geo-bbox"      => manhattan_bbox,
          })) do |msg|
            msgs.send msg
          end

          # Should receive the two Manhattan messages
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "times_square"
          msg1.ack

          msg2 = msgs.receive
          msg2.body_io.to_s.should eq "central_park"
          msg2.ack

          # Should NOT receive Brooklyn message
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should filter messages by polygon" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-polygon", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Define triangular delivery zone
          delivery_zone = {
            "points" => [
              [40.7000, -74.0000],
              [40.8000, -74.0000],
              [40.7500, -73.9000],
            ],
          }

          # Message 1: Inside triangle - should match
          q.publish("inside", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7500,
              "x-geo-lon" => -73.9700,
            })
          ))

          # Message 2: Outside triangle - should NOT match
          q.publish("outside", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.6000,
              "x-geo-lon" => -74.0000,
            })
          ))

          # Subscribe with polygon filter
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset" => "first",
            "x-geo-polygon"   => delivery_zone,
          })) do |msg|
            msgs.send msg
          end

          # Should receive only the inside message
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "inside"
          msg1.ack

          # Should NOT receive outside message
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should combine GIS and standard filters with ALL logic" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-combined", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Message 1: Matches location AND category - should match
          q.publish("pizza_nearby", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.7580,
              "x-geo-lon"             => -73.9855,
              "x-stream-filter-value" => "food",
            })
          ))

          # Message 2: Matches location but NOT category - should NOT match
          q.publish("museum_nearby", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.7484,
              "x-geo-lon"             => -73.9857,
              "x-stream-filter-value" => "museum",
            })
          ))

          # Message 3: Matches category but NOT location - should NOT match
          q.publish("pizza_far", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.6782,
              "x-geo-lon"             => -73.9442,
              "x-stream-filter-value" => "food",
            })
          ))

          # Subscribe with both GIS and standard filters (ALL logic)
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset"     => "first",
            "x-stream-filter"     => "food",
            "x-filter-match-type" => "ALL",
            "x-geo-within-radius" => {
              "lat"       => 40.7580,
              "lon"       => -73.9855,
              "radius_km" => 5.0,
            },
          })) do |msg|
            msgs.send msg
          end

          # Should receive only the message matching both filters
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "pizza_nearby"
          msg1.ack

          # Should NOT receive other messages
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should combine GIS and standard filters with ANY logic" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-any", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Message 1: Matches location AND category - should match
          q.publish("pizza_nearby", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.7580,
              "x-geo-lon"             => -73.9855,
              "x-stream-filter-value" => "food",
            })
          ))

          # Message 2: Matches location but NOT category - should match
          q.publish("museum_nearby", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.7484,
              "x-geo-lon"             => -73.9857,
              "x-stream-filter-value" => "museum",
            })
          ))

          # Message 3: Matches category but NOT location - should match
          q.publish("pizza_far", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.6782,
              "x-geo-lon"             => -73.9442,
              "x-stream-filter-value" => "food",
            })
          ))

          # Message 4: Matches neither - should NOT match
          q.publish("museum_far", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat"             => 40.6782,
              "x-geo-lon"             => -73.9442,
              "x-stream-filter-value" => "museum",
            })
          ))

          # Subscribe with both GIS and standard filters (ANY logic)
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset"     => "first",
            "x-stream-filter"     => "food",
            "x-filter-match-type" => "ANY",
            "x-geo-within-radius" => {
              "lat"       => 40.7580,
              "lon"       => -73.9855,
              "radius_km" => 5.0,
            },
          })) do |msg|
            msgs.send msg
          end

          # Should receive first three messages (match at least one filter)
          received = [] of String
          3.times do
            msg = msgs.receive
            received << msg.body_io.to_s
            msg.ack
          end

          received.should contain "pizza_nearby"
          received.should contain "museum_nearby"
          received.should contain "pizza_far"

          # Should NOT receive museum_far
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should handle multiple GIS filters with ALL logic" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-multi", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Message 1: Inside both radius and bbox - should match
          q.publish("inside_both", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7580,
              "x-geo-lon" => -73.9855,
            })
          ))

          # Message 2: Inside radius but outside bbox - should NOT match
          q.publish("radius_only", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7484,
              "x-geo-lon" => -74.0500, # Outside bbox
            })
          ))

          # Subscribe with both radius and bbox filters (ALL logic)
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset"     => "first",
            "x-filter-match-type" => "ALL",
            "x-geo-within-radius" => {
              "lat"       => 40.7580,
              "lon"       => -73.9855,
              "radius_km" => 10.0,
            },
            "x-geo-bbox" => {
              "min_lat" => 40.7000,
              "max_lat" => 40.8000,
              "min_lon" => -74.0200,
              "max_lon" => -73.9000,
            },
          })) do |msg|
            msgs.send msg
          end

          # Should receive only message matching both filters
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "inside_both"
          msg1.ack

          # Should NOT receive radius_only message
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should reject messages without geo coordinates" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-no-coords", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Message 1: Has coordinates - should match
          q.publish("with_coords", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "x-geo-lat" => 40.7580,
              "x-geo-lon" => -73.9855,
            })
          ))

          # Message 2: No coordinates - should NOT match
          q.publish("no_coords", props: AMQP::Client::Properties.new(
            headers: AMQP::Client::Arguments.new({
              "some-other-header" => "value",
            })
          ))

          # Subscribe with GIS filter
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
            "x-stream-offset"     => "first",
            "x-geo-within-radius" => {
              "lat"       => 40.7580,
              "lon"       => -73.9855,
              "radius_km" => 10.0,
            },
          })) do |msg|
            msgs.send msg
          end

          # Should receive only message with coordinates
          msg1 = msgs.receive
          msg1.body_io.to_s.should eq "with_coords"
          msg1.ack

          # Should NOT receive message without coordinates
          select
          when msg = msgs.receive
            fail "Should not receive message: #{msg.body_io.to_s}"
          when timeout(100.milliseconds)
            # Expected - no more messages
          end

          q.delete
        end
      end
    end

    it "should raise error for invalid GIS filter parameters" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("gis-invalid", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))

          # Invalid radius filter (missing radius_km)
          expect_raises(AMQP::Client::Channel::ClosedException, /x-geo-within-radius/) do
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({
              "x-stream-offset"     => "first",
              "x-geo-within-radius" => {
                "lat" => 40.7580,
                "lon" => -73.9855,
                # Missing radius_km
              },
            })) { }
          end
        end
      end
    end
  end
end
