module LavinMQ
  module SortableJSON
    abstract def details_tuple

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end

    protected def search_value
      @name
    end

    def search_match?(value : String)
      search_value.includes? value
    end

    def search_match?(value : Regex)
      value === search_value
    end
  end
end
