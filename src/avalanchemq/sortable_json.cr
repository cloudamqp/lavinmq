module AvalancheMQ
  module SortableJSON
    abstract def details_tuple

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end

    macro mapping(**_properties_)
      ::JSON.mapping({{_properties_}})
      def details_tuple
        { {% for key, value in _properties_ %}
          {{value[:key] || key.id}}: @{{key.id}},
        {% end %} }
      end
    end
  end
end
