require "./error"

class NameValidator
  PREFIX_LIST = ["mqtt.", "amq."]

  def self.reserved_prefix?(name)
    PREFIX_LIST.any? { |prefix| name.starts_with? prefix }
  end

  def self.valid_entity_name?(name) : Bool
    return true if name.empty?
    name.matches?(/\A[ -~]*\z/)
  end
end
