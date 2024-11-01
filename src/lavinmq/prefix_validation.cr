require "./error"

class PrefixValidation
  PREFIX_LIST = ["mqtt.", "amq."]

  def self.invalid?(name)
    prefix = name[0..name.index(".") || name.size - 1]
    return true if PREFIX_LIST.includes?(prefix)
    return false
  end
end
