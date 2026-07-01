require "json"

module LavinMQ
  module Auth
    class PermissionGroup
      include JSON::Serializable

      struct Rule
        include JSON::Serializable
        getter pattern : String
        getter? read : Bool = false
        getter? write : Bool = false

        def initialize(@pattern : String, @read : Bool = false, @write : Bool = false)
        end
      end

      getter name : String
      getter protocol : String
      getter? apply_to_all : Bool
      getter members : Array(String)
      getter rules : Array(Rule)

      def initialize(@name : String,
                     @protocol : String = "mqtt",
                     @apply_to_all : Bool = false,
                     @members : Array(String) = [] of String,
                     @rules : Array(Rule) = [] of Rule)
      end

      def member?(username : String) : Bool
        @apply_to_all || @members.includes?(username)
      end
    end
  end
end
