require "json"
require "amq-protocol"
require "../error"

module LavinMQ
  module QueueFilter
    REPLAY_HEADER = "x-source-queue"

    enum Op
      Eq
      NotEq
      Exists
    end

    enum XMatch
      All
      Any
    end

    enum Action
      Drop
      MoveTo
      DuplicateTo
    end

    struct Clause
      include JSON::Serializable

      getter key : String
      getter op : Op
      getter value : String?

      def initialize(@key : String, @op : Op, @value : String? = nil)
      end

      def match?(headers : AMQ::Protocol::Table?) : Bool
        case @op
        in .exists?
          !headers.nil? && headers.has_key?(@key)
        in .eq?
          return false if headers.nil?
          msg_value = headers[@key]?
          return false if msg_value.nil?
          msg_value.to_s == @value
        in .not_eq?
          return true if headers.nil?
          msg_value = headers[@key]?
          return true if msg_value.nil?
          msg_value.to_s != @value
        end
      end
    end

    struct Rule
      include JSON::Serializable

      getter clauses : Array(Clause)

      @[JSON::Field(key: "x-match")]
      getter x_match : XMatch = XMatch::All

      getter action : Action = Action::Drop
      getter target : String? = nil
      getter rule_id : String? = nil

      def initialize(@clauses : Array(Clause), @x_match : XMatch = XMatch::All,
                     @action : Action = Action::Drop, @target : String? = nil,
                     @rule_id : String? = nil)
      end

      def validate! : Nil
        if @clauses.empty?
          raise LavinMQ::Error::PreconditionFailed.new("queue filter must have at least one clause")
        end
        if needs_target? && (@target.nil? || @target.try(&.empty?))
          raise LavinMQ::Error::PreconditionFailed.new("action '#{@action.to_s.downcase}' requires a non-empty target queue")
        end
        @clauses.each do |c|
          if (c.op.eq? || c.op.not_eq?) && c.value.nil?
            raise LavinMQ::Error::PreconditionFailed.new("clause '#{c.key}' with op '#{c.op.to_s.downcase}' requires a value")
          end
        end
      end

      def match?(properties : AMQ::Protocol::Properties) : Bool
        headers = properties.headers
        return false if headers && headers.has_key?(REPLAY_HEADER)
        case @x_match
        in .all?
          @clauses.all?(&.match?(headers))
        in .any? # ameba:disable Performance/AnyInsteadOfEmpty
          @clauses.any?(&.match?(headers))
        end
      end

      def self.parse(json : String) : Rule
        r = Rule.from_json(json)
        r.validate!
        r
      end

      private def needs_target? : Bool
        @action.move_to? || @action.duplicate_to?
      end
    end
  end
end
