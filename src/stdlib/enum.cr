struct Enum
  def self.from_value?(value : Int) : self?
    {% if @type.annotation(Flags) %}
      all_mask = {{@type}}::All.value
      return if all_mask & value != value
      return new(value)
    {% else %}
      {% for member in @type.constants %}
        return new({{@type.constant(member)}}) if {{@type.constant(member)}} == value
      {% end %}
    {% end %}
    nil
  end
end
