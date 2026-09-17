require "json"
require "./errors"
require "../auth/user"

module LavinMQ
  module SQS
    # A protocol-neutral SQS request. The wire protocol layer (JSON 1.0 today,
    # Query later) parses the incoming HTTP request into this shape so each
    # action is implemented once.
    struct Request
      getter action : String
      getter params : JSON::Any
      getter user : Auth::User
      # scheme://host[:port] used when minting queue URLs
      getter base_url : String
      # vhost selected by the request path when the action has no QueueUrl
      getter path_vhost : String
      # region from the SigV4 credential scope, echoed back in ARNs
      getter region : String
      getter request_id : String

      def initialize(@action, @params, @user, @base_url, @path_vhost, @region, @request_id)
      end

      def has?(key : String) : Bool
        value = @params[key]?
        !(value.nil? || value.raw.nil?)
      end

      def string?(key : String) : String?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        value.as_s? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a string.")
      end

      def string(key : String) : String
        string?(key) || raise MissingParameter.new("The request must contain the parameter #{key}.")
      end

      def int?(key : String) : Int64?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        case raw = value.raw
        when Int    then raw.to_i64
        when Float  then raw.to_i64 if raw == raw.floor
        when String then raw.to_i64?
        end || raise InvalidParameterValue.new("Value #{value} for parameter #{key} is invalid. Reason: Must be an integer.")
      end

      def int(key : String, range : Range(Int32, Int32), default : Int32? = nil) : Int32
        value = int?(key)
        if value.nil?
          return default unless default.nil?
          raise MissingParameter.new("The request must contain the parameter #{key}.")
        end
        unless range.includes?(value)
          raise InvalidParameterValue.new("Value #{value} for parameter #{key} is invalid. Reason: Must be between #{range.begin} and #{range.end}.")
        end
        value.to_i32
      end

      def strings?(key : String) : Array(String)?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        array = value.as_a? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a list.")
        array.map do |item|
          item.as_s? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a list of strings.")
        end
      end

      def string_map?(key : String) : Hash(String, String)?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        hash = value.as_h? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a map.")
        hash.transform_values do |item|
          item.as_s? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a map of strings.")
        end
      end

      def hash?(key : String) : Hash(String, JSON::Any)?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        value.as_h? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a map.")
      end

      def array?(key : String) : Array(JSON::Any)?
        value = @params[key]?
        return if value.nil? || value.raw.nil?
        value.as_a? || raise InvalidParameterValue.new("Value for parameter #{key} is invalid. Reason: Must be a list.")
      end
    end
  end
end
