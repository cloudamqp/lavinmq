module AvalancheMQ
  module SortableJSON
    abstract def details_tuple

    def to_json(json : JSON::Builder)
      details_tuple.to_json(json)
    end

    macro mapping(**_properties_)
      ::JSON.mapping({{_properties_}})
      def details_tuple
        { {% for key, _value in _properties_ %}
          {{key.id}}: @{{key.id}},
        {% end %} }
      end
    end
  end
end
