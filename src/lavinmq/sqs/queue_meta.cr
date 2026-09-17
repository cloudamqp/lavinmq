require "json"
require "./errors"
require "../rough_time"

module LavinMQ
  module SQS
    # SQS attributes and tags for a queue, kept next to the AMQP queue that
    # holds the messages. Stored per vhost by `QueueMetaStore`.
    class QueueMeta
      include JSON::Serializable

      property name : String
      property attributes : Hash(String, String)
      property tags : Hash(String, String)
      property created_timestamp : Int64
      property last_modified_timestamp : Int64

      def initialize(@name : String, attributes = Hash(String, String).new,
                     @tags = Hash(String, String).new, now = RoughTime.unix_ms // 1000)
        @attributes = attributes.dup
        @created_timestamp = now
        @last_modified_timestamp = now
      end

      def fifo? : Bool
        @attributes["FifoQueue"]? == "true"
      end

      def visibility_timeout : Int32
        int("VisibilityTimeout")
      end

      def receive_wait_time : Int32
        int("ReceiveMessageWaitTimeSeconds")
      end

      def delay_seconds : Int32
        int("DelaySeconds")
      end

      def maximum_message_size : Int32
        int("MaximumMessageSize")
      end

      def message_retention_period : Int32
        int("MessageRetentionPeriod")
      end

      def content_based_deduplication? : Bool
        @attributes["ContentBasedDeduplication"]? == "true"
      end

      def touch : Nil
        @last_modified_timestamp = RoughTime.unix_ms // 1000
      end

      # Stored attributes merged with the defaults for the ones not set.
      def effective_attributes : Hash(String, String)
        QueueAttributes::DEFAULTS.merge(@attributes)
      end

      private def int(key : String) : Int32
        (@attributes[key]? || QueueAttributes::DEFAULTS[key]).to_i32
      end
    end

    module QueueAttributes
      DEFAULTS = {
        "DelaySeconds"                  => "0",
        "MaximumMessageSize"            => "262144",
        "MessageRetentionPeriod"        => "345600",
        "ReceiveMessageWaitTimeSeconds" => "0",
        "VisibilityTimeout"             => "30",
      }

      INTEGER_RANGES = {
        "DelaySeconds"                  => 0..900,
        "MaximumMessageSize"            => 1024..1_048_576,
        "MessageRetentionPeriod"        => 60..1_209_600,
        "ReceiveMessageWaitTimeSeconds" => 0..20,
        "VisibilityTimeout"             => 0..43_200,
        "KmsDataKeyReusePeriodSeconds"  => 60..86_400,
      }

      BOOLEANS = {"FifoQueue", "ContentBasedDeduplication", "SqsManagedSseEnabled"}

      # Accepted and stored, but not (yet) enforced by the broker
      PASSTHROUGH = {"Policy", "RedrivePolicy", "RedriveAllowPolicy", "KmsMasterKeyId",
                     "DeduplicationScope", "FifoThroughputLimit"}

      CREATE_ONLY = {"FifoQueue"}

      # Attributes computed at request time, never stored
      COMPUTED = {"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                  "ApproximateNumberOfMessagesDelayed", "CreatedTimestamp",
                  "LastModifiedTimestamp", "QueueArn"}

      def self.known?(name : String) : Bool
        INTEGER_RANGES.has_key?(name) || BOOLEANS.includes?(name) || PASSTHROUGH.includes?(name)
      end

      def self.readable?(name : String) : Bool
        known?(name) || COMPUTED.includes?(name)
      end

      def self.validate!(name : String, value : String) : Nil
        if range = INTEGER_RANGES[name]?
          int = value.to_i32? || raise InvalidAttributeValue.new("Invalid value for the parameter #{name}. Reason: Must be an integer.")
          unless range.includes?(int)
            raise InvalidAttributeValue.new("Invalid value for the parameter #{name}. Reason: Must be an integer from #{range.begin} to #{range.end} seconds.")
          end
        elsif BOOLEANS.includes?(name)
          unless value == "true" || value == "false"
            raise InvalidAttributeValue.new("Invalid value for the parameter #{name}. Reason: Must be true or false.")
          end
        elsif PASSTHROUGH.includes?(name)
          if name.ends_with?("Policy") && !value.empty?
            JSON.parse(value) rescue raise InvalidAttributeValue.new("Invalid value for the parameter #{name}. Reason: Must be valid JSON.")
          end
        else
          raise InvalidAttributeName.new("Unknown Attribute #{name}.")
        end
      end

      def self.validate_all!(attributes : Hash(String, String)) : Nil
        attributes.each { |name, value| validate!(name, value) }
      end
    end
  end
end
