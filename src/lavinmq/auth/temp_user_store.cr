require "./users/temp_user"

module LavinMQ
  module Auth
    class TempUserStore
      Log = LavinMQ::Log.for "temp_user_store"

      def initialize
        @users = Hash(String, Users::TempUser).new
        spawn cleanup_expired_users
      end

      def []?(name : String) : Users::TempUser?
        user = @users[name]?
        return nil unless user
        if user.expired?
          @users.delete(name)
          Log.debug { "Removed expired temp user=#{name}" }
          nil
        else
          user
        end
      end

      def add(user : Users::TempUser)
        @users[user.name] = user
        Log.info { "Added temp user=#{user.name}" }
      end

      def delete(name : String) : Users::TempUser?
        if user = @users.delete(name)
          Log.info { "Deleted temp user=#{name}" }
          user
        end
      end

      def size
        @users.size
      end

      def each_value
        @users.each_value.reject(&.expired?)
      end

      private def cleanup_expired_users
        loop do
          sleep 60.seconds
          expired = @users.select { |_, u| u.expired? }
          expired.each_key { |name| @users.delete(name) }
          Log.debug { "Cleaned up #{expired.size} expired temp users" } if expired.any?
        end
      end
    end
  end
end
