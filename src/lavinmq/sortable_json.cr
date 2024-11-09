require "./metadata"

module LavinMQ
  module SortableJSON
    abstract def details_tuple

    def metadata
      Metadata.new details_tuple
    end

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end
  end
end
