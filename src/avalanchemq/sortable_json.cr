module AvalancheMQ
  module SortableJSON
    abstract def details_tuple

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end
  end
end
