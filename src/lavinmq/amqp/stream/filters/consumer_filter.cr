require "./filter"
require "./kv"
require "./x_stream_filter"
require "./gis"

module LavinMQ::AMQP
  # The filters of a stream consumer, from its `x-stream-filter`,
  # `x-filter-match-type` and `x-stream-match-unfiltered` arguments
  struct ConsumerFilter
    def initialize(@filters : Array(StreamFilter), @match_all : Bool, @match_unfiltered : Bool)
    end

    def self.from_arguments(arguments : AMQ::Protocol::Table) : self
      new(StreamFilter.from_arguments(arguments), match_all(arguments), match_unfiltered(arguments))
    end

    def match?(headers : AMQP::Table?) : Bool
      return true if @filters.empty?
      if @match_unfiltered
        return true unless headers.try &.has_key?("x-stream-filter-value")
      end
      return false unless headers
      @match_all ? @filters.all?(&.match?(headers)) : @filters.any?(&.match?(headers))
    end

    private def self.match_all(arguments) : Bool
      case match_type = arguments["x-filter-match-type"]?
      when Nil then true
      when String
        case match_type.downcase
        when "all" then true
        when "any" then false
        else            raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
        end
      else raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
      end
    end

    private def self.match_unfiltered(arguments) : Bool
      case match_unfiltered = arguments["x-stream-match-unfiltered"]?
      when Nil  then false
      when Bool then match_unfiltered
      else           raise LavinMQ::Error::PreconditionFailed.new("x-stream-match-unfiltered must be a boolean")
      end
    end
  end
end
