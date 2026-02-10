module LavinMQ
  @[Flags]
  enum Tag : UInt8
    Administrator
    Monitoring
    Management
    PolicyMaker
    Impersonator

    def self.parse_list(list : String) : Array(Tag)
      list.split(",").compact_map { |t| Tag.parse?(t.strip) }
    end

    # This is to not have to allocate memory we serialize it or what ever
    def to_downcase_s
      {% begin %}
        case self
          {% for member in @type.constants %}
            {% unless %w(none all).includes?(member.stringify.downcase) %}
              when .{{member.id.underscore}}?
                {{member.stringify.downcase}}
            {% end %}
          {% end %}
        else #  should we every reach this?
          self.to_s.downcase
        end
      {% end %}
    end
  end
end
