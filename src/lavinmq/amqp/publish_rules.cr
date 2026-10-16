require "amq-protocol"
require "./channel_reply_code"
require "../config"
require "../rough_time"

module LavinMQ
  module AMQP
    # The checks every publish to a vhost goes through, whether it arrives on
    # an AMQP channel or from an in-process shovel or federation link
    # (Endpoint::LocalSession). A refused publish is a Refusal, with the
    # reply code an AMQP channel closes with; the session raises it instead.
    module PublishRules
      record Refusal, code : ChannelReplyCode, text : String

      # Checks a publish before its properties and body are known
      def self.check_target(vhost, exchange_name : String) : Refusal?
        check_flow(vhost) || check_exchange(vhost, exchange_name)
      end

      # Publishes are refused while the server is low on disk space
      def self.check_flow(vhost) : Refusal?
        unless vhost.flow?
          Refusal.new(ChannelReplyCode::PRECONDITION_FAILED, "Server low on disk space")
        end
      end

      def self.check_exchange(vhost, exchange_name : String) : Refusal?
        ex = vhost.exchange?(exchange_name)
        unless ex
          return Refusal.new(ChannelReplyCode::NOT_FOUND, "No exchange '#{exchange_name}' in vhost '#{vhost.name}'")
        end
        if ex.internal?
          Refusal.new(ChannelReplyCode::ACCESS_REFUSED, "Exchange '#{exchange_name}' in vhost '#{vhost.name}' is internal")
        end
      end

      # Checks the properties and size of a message
      def self.check_message(properties : AMQ::Protocol::Properties, body_size : UInt64) : Refusal?
        if exp = properties.expiration
          if i = exp.to_i?
            if i < 0
              return Refusal.new(ChannelReplyCode::PRECONDITION_FAILED, "Negative expiration not allowed")
            end
          else
            return Refusal.new(ChannelReplyCode::PRECONDITION_FAILED, "Expiration not a number")
          end
        end
        max = Config.instance.max_message_size
        if body_size > max
          Refusal.new(ChannelReplyCode::PRECONDITION_FAILED, "message size #{body_size} larger than max size #{max}")
        end
      end

      # Sets what the broker adds to every message it accepts
      def self.prepare(properties : AMQ::Protocol::Properties) : AMQ::Protocol::Properties
        if properties.timestamp_raw.nil? && Config.instance.set_timestamp?
          properties.timestamp = RoughTime.utc
        end
        properties
      end
    end
  end
end
