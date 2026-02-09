module LavinMQ
  module Auth
    record PermissionKey, vhost : String, name : String

    class PermissionCache
      @cache = Hash(PermissionKey, Bool).new
      property revision = 0_u32

      def []?(key : PermissionKey) : Bool?
        @cache[key]?
      end

      def []=(key : PermissionKey, value : Bool) : Bool
        @cache[key] = value
      end

      def size
        @cache.size
      end

      def clear
        @cache.clear
      end
    end
  end
end
