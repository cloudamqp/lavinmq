require "sync/exclusive"

module LavinMQ
  module Auth
    record PermissionKey, vhost : String, name : String

    class PermissionCache
      @cache : Sync::Exclusive(Hash(PermissionKey, Bool)) = Sync::Exclusive.new(Hash(PermissionKey, Bool).new)
      @revision = Atomic(UInt32).new(0_u32)

      def revision : UInt32
        @revision.get(:relaxed)
      end

      def revision=(value : UInt32)
        @revision.set(value, :relaxed)
      end

      def []?(key : PermissionKey) : Bool?
        @cache.lock { |h| h[key]? }
      end

      def []=(key : PermissionKey, value : Bool) : Bool
        @cache.lock { |h| h[key] = value }
      end

      def size
        @cache.lock(&.size)
      end

      def clear
        @cache.lock(&.clear)
      end
    end
  end
end
