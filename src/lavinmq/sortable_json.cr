module LavinMQ
  module SortableJSON
    abstract def details_tuple

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end

    def search_match?(value : String)
      @name.includes? value
    end

    def search_match?(value : Regex)
      value === @name
    end
  end
end
