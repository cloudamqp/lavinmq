module LavinMQ
  module Auth
    record PermissionKey, vhost : String, name : String

    class PermissionCache
      @cache = Hash(PermissionKey, Bool).new
      property revision = 0_u32

      forward_missing_to @cache
    end
  end
end
