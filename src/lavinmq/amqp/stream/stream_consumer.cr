require "../consumer"
require "../../segment_position"
require "./gis_filter"

module LavinMQ
  module AMQP
    class StreamConsumer < Consumer
      include SortableJSON
      property offset : Int64
      property segment : UInt32
      property pos : UInt32
      getter requeued = Deque(SegmentPosition).new
      @consumer_filters = Array(Tuple(String, String)).new
      @filter_match_all = true
      @match_unfiltered = false
      @track_offset = false
      # GIS filter storage
      @gis_radius_filter : Tuple(StreamGISFilter::Point, Float64)?
      @gis_bbox_filter : StreamGISFilter::BoundingBox?
      @gis_polygon_filter : StreamGISFilter::Polygon?

      def initialize(@channel : Client::Channel, @queue : Stream, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        validate_preconditions(frame)
        offset = frame.arguments["x-stream-offset"]?
        @offset, @segment, @pos = stream_queue.find_offset(offset, @tag, @track_offset)
        super
        @new_message_available = BoolChannel.new(false)
      end

      private def validate_preconditions(frame)
        if frame.exclusive
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must not be exclusive")
        end
        if frame.no_ack
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must acknowledge messages")
        end
        if @channel.prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must have a prefetch limit")
        end
        unless @channel.global_prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers does not support global prefetch limit")
        end
        if frame.arguments.has_key? "x-priority"
          raise LavinMQ::Error::PreconditionFailed.new("x-priority not supported on streams")
        end
        validate_stream_offset(frame)
        validate_stream_filter(frame.arguments["x-stream-filter"]?)
        validate_gis_filters(frame.arguments)
        validate_filter_match_type(frame)
        case match_unfiltered = frame.arguments["x-stream-match-unfiltered"]?
        when Bool
          @match_unfiltered = match_unfiltered
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-match-unfiltered must be a boolean")
        end
      end

      private def validate_stream_offset(frame)
        case frame.arguments["x-stream-offset"]?
        when Nil
          @track_offset = true unless @tag.starts_with?("amq.ctag-")
        when Int, Time, "first", "next", "last"
          case frame.arguments["x-stream-automatic-offset-tracking"]?
          when Bool
            @track_offset = frame.arguments["x-stream-automatic-offset-tracking"]?.as(Bool)
          when String
            @track_offset = frame.arguments["x-stream-automatic-offset-tracking"]? == "true"
          end
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
        end
      end

      private def validate_stream_filter(arg)
        case arg
        when String
          arg.split(',').each do |f|
            @consumer_filters << {"x-stream-filter", f.strip}
          end
        when AMQ::Protocol::Table
          arg.each do |k, v|
            if k.to_s == "x-stream-filter"
              v.to_s.split(',').each do |f|
                @consumer_filters << {k.to_s, f.strip}
              end
            else
              @consumer_filters << {k.to_s, v.to_s}
            end
          end
        when Array
          arg.each do |f|
            validate_stream_filter(f)
          end
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-filter must be a string, table, or array")
        end
      end

      private def validate_gis_filters(arguments : AMQ::Protocol::Table)
        # Validate and store radius filter
        if radius_arg = arguments["x-geo-within-radius"]?
          @gis_radius_filter = StreamGISFilter.parse_radius_filter(radius_arg)
          if @gis_radius_filter.nil?
            raise LavinMQ::Error::PreconditionFailed.new(
              "x-geo-within-radius must be a table with 'lat', 'lon', and 'radius_km' (positive number)"
            )
          end
        end

        # Validate and store bounding box filter
        if bbox_arg = arguments["x-geo-bbox"]?
          @gis_bbox_filter = StreamGISFilter.parse_bbox_filter(bbox_arg)
          if @gis_bbox_filter.nil?
            raise LavinMQ::Error::PreconditionFailed.new(
              "x-geo-bbox must be a table with 'min_lat', 'max_lat', 'min_lon', and 'max_lon'"
            )
          end
        end

        # Validate and store polygon filter
        if polygon_arg = arguments["x-geo-polygon"]?
          @gis_polygon_filter = StreamGISFilter.parse_polygon_filter(polygon_arg)
          if @gis_polygon_filter.nil?
            raise LavinMQ::Error::PreconditionFailed.new(
              "x-geo-polygon must be a table with 'points' array of [lat, lon] pairs (at least 3 points)"
            )
          end
        end
      end

      private def validate_filter_match_type(frame)
        case filter_match_type = frame.arguments["x-filter-match-type"]?
        when String
          if filter_match_type.downcase == "all"
            @filter_match_all = true
          elsif filter_match_type.downcase == "any"
            @filter_match_all = false
          else
            raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
          end
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
        end
      end

      private def deliver_loop
        i = 0
        loop do
          wait_for_capacity
          loop do
            raise ClosedError.new if @closed
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          stream_queue.consume_get(self) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue ex : ClosedError | Queue::ClosedError | AMQP::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      end

      private def wait_for_queue_ready
        if @offset > stream_queue.last_offset && @requeued.empty?
          @log.debug { "Waiting for queue not to be empty" }
          flush
          select
          when @new_message_available.when_true.receive
            @log.debug { "Queue is not empty - new message notification received" }
            @new_message_available.set(false) # Reset the flag
          when @notify_closed.receive
          end
          return true
        end
      end

      def notify_new_message
        @new_message_available.set(true)
      end

      private def stream_queue : Stream
        @queue.as(Stream)
      end

      def waiting_for_messages?
        (@offset + @prefetch_count) >= stream_queue.last_offset && accepts?
      end

      def ack(sp)
        stream_queue.store_consumer_offset(@tag, @offset) if @track_offset
        super
      end

      def reject(sp, requeue : Bool)
        super
        if requeue
          @requeued.push(sp)
          @new_message_available.set(true) if @requeued.size == 1
        end
      end

      def close
        @new_message_available.close
        super
      end

      def filter_match?(msg_headers) : Bool
        has_standard_filters = !@consumer_filters.empty?
        has_gis_filters = @gis_radius_filter || @gis_bbox_filter || @gis_polygon_filter

        # No filters at all - always match
        return true unless has_standard_filters || has_gis_filters

        # Handle unfiltered message matching (only for standard filters)
        if @match_unfiltered && has_standard_filters && !has_gis_filters
          return true unless msg_headers.try &.has_key?("x-stream-filter-value")
        end

        return false unless headers = msg_headers

        # If only standard filters exist, use original logic
        return filter_match_standard?(headers) unless has_gis_filters

        # If only GIS filters exist, use GIS logic
        return match_gis_filters?(headers) unless has_standard_filters

        @log.debug { "Both standard and GIS filters present - combining results" }

        # Both filter types exist - combine them
        standard_match = filter_match_standard?(headers)
        gis_match = match_gis_filters?(headers)

        case @filter_match_all
        when false # ANY: At least one filter type must match
          standard_match || gis_match
        else # ALL: All filter types must match
          standard_match && gis_match
        end
      end

      private def filter_match_standard?(headers : AMQ::Protocol::Table) : Bool
        case @filter_match_all
        when false # ANY: Return true on first match
          @consumer_filters.any? { |key, value| match_header_filter?(key, value, headers) }
        else # ALL: Return false on first non-match
          @consumer_filters.all? { |key, value| match_header_filter?(key, value, headers) }
        end
      end

      private def match_header_filter?(key, value, msg_headers) : Bool
        return msg_headers[key]? == value if key != "x-stream-filter"

        if msg_filter_values = msg_headers.try &.fetch("x-stream-filter-value", nil).try &.to_s
          msg_filter_values.split(',') do |msg_filter_value|
            return true if msg_filter_value == value
          end
        end
        false
      end

      private def match_gis_filters?(msg_headers : AMQ::Protocol::Table) : Bool
        # If no GIS filters are set, they pass
        return true unless @gis_radius_filter || @gis_bbox_filter || @gis_polygon_filter

        # Check each GIS filter type
        results = [] of Bool

        if radius_filter = @gis_radius_filter
          center, radius_km = radius_filter
          results << StreamGISFilter.match_radius?(msg_headers, center, radius_km)
        end

        if bbox_filter = @gis_bbox_filter
          results << StreamGISFilter.match_bbox?(msg_headers, bbox_filter)
        end

        if polygon_filter = @gis_polygon_filter
          results << StreamGISFilter.match_polygon?(msg_headers, polygon_filter)
        end

        # Apply ALL/ANY logic to GIS filters
        case @filter_match_all
        when false # ANY: At least one GIS filter must match
          results.any?
        else
          results.all?
        end
      end
    end
  end
end
