require "./error"

class NameValidator
  PREFIX_LIST = ["mqtt.", "amq."]

  def self.valid_prefix?(name)
    return true if PREFIX_LIST.any? { |prefix| name.starts_with? prefix }
    return false
  end

  def self.valid_entity_name(name) : Bool
    return true if name.empty?
    name.matches?(/\A[ -~]*\z/)
  end
end
