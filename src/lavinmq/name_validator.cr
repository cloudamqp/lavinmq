require "./error"

class NameValidator
  PREFIX_LIST = ["mqtt.", "amq."]

  def self.valid_prefix?(name)
    prefix = name[0..name.index(".") || name.size - 1]
    return true if PREFIX_LIST.includes?(prefix)
    return false
  end

  def self.valid_entity_name(name) : Bool
    return true if name.empty?
    name.matches?(/\A[ -~]*\z/)
  end
end
