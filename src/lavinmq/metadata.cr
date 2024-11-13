module AMQ::Protocol
  struct Table
  end
end

module LavinMQ
  abstract struct Metadata
    def self.new(data : NamedTuple)
      NamedTupleMetadata.new(data)
    end

    # This is similar to JSON::Any..
    struct Value(T) # , V)
      include Comparable(Value)

      def initialize(@value : T)
        @type = T
      end

      def self.nil
        new(nil)
      end

      def type
        T
      end

      def value : T
        @value
      end

      def <=>(other : Value)
        return 0 if @value.nil? && other.@value.nil?
        return -1 if @value.nil?
        return 1 if other.@value.nil?

        if Number === @value === other.@value
          return @value <=> other.@value
        end
        if self.type != other.type
          return @value.to_s <=> other.@value.to_s
        end

        if (value = @value) && (other_value = other.@value.as?(T))
          if value.is_a?(Comparable)
            return value <=> other_value
          end
        end

        return 0
      end

      delegate to_json, to_s, to: @value
    end
  end

  # Wraps a generic NamedTuple to make it possible to add
  # methods to it. Used in e.g. HTTP::Controller.
  struct NamedTupleMetadata(T) < Metadata
    def initialize(@data : T)
    end

    def self.empty
      new NamedTuple.new
    end

    delegate to_json, to_s, to: @data

    # Takes a dot separated path and returns the value at that path
    # If T is `{a: {b: {c: 1} d: "foo"}` #dig("a.b.c") returns a Value(Int32)
    # and #dig("a.d") returns a Value(String)
    def dig(path : Symbol | String)
      fetch(path) { raise KeyError.new "Invalid path: #{path.inspect}" }
    end

    def dig?(path : Symbol | String)
      fetch(path) { Value.nil }
    end

    def [](key : Symbol | String)
      fetch(key) { raise KeyError.new "Missing key: #{key.inspect}" }
    end

    def []?(key : Symbol | String)
      fetch(key) { Value.nil }
    end

    private def fetch(path : Symbol | String, &default : -> Value)
      {% begin %}
        {%
          paths = [] of Array(String)
          # This will walk the "namedtuple tree" and find all paths to values. It's not
          # possible to do recursive macros, but ArrayLiteral#each will iterate over items
          # added while currently iterating.
          to_visit = T.keys.map { |k| {[k], T[k]} }
          to_visit.each do |(path, type)|
            if type <= NamedTuple
              paths << path
              type.keys.each { |k| to_visit << {path + [k], type[k]} }
            else
              paths << path
            end
          end
        %}
        case path
          {% for path in paths %}
            when {{path.join(".")}}, :{{path.join(".")}}
              if value = @data[:"{{path.join("\"][:\"").id}}"]
                return Value.new(value)
              end
              return Value.nil
          {% end %}
        else
          yield
        end
      {% end %}
    end
  end
end
