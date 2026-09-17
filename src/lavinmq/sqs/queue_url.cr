require "uri"
require "./errors"

module LavinMQ
  module SQS
    # Queue URLs are minted by the server and passed back opaquely by the SDK:
    #
    #   http(s)://<host>:<port>/<vhost-segment>/<queue-name>
    #
    # The vhost segment is the URI-encoded vhost name. The default vhost `/` is
    # written as the fake AWS account id `000000000000`, and any 12-digit
    # account id is accepted as an alias for it so hard-coded ElasticMQ-style
    # URLs keep working.
    module QueueUrl
      DEFAULT_ACCOUNT_ID = "000000000000"
      ACCOUNT_ID_PATTERN = /\A\d{12}\z/

      def self.vhost_segment(vhost : String) : String
        vhost == "/" ? DEFAULT_ACCOUNT_ID : URI.encode_path_segment(vhost)
      end

      def self.vhost_from_segment(segment : String) : String
        decoded = URI.decode(segment)
        decoded.matches?(ACCOUNT_ID_PATTERN) ? "/" : decoded
      end

      def self.build(base_url : String, vhost : String, queue_name : String) : String
        "#{base_url}/#{vhost_segment(vhost)}/#{queue_name}"
      end

      # Returns {vhost, queue_name}
      def self.parse(url : String) : Tuple(String, String)
        uri = URI.parse(url)
        path = uri.path
        raise InvalidAddress.new("The address #{url} is not valid for this endpoint.") if path.nil? || uri.host.nil?
        parts = path.split('/', remove_empty: true)
        raise InvalidAddress.new("The address #{url} is not valid for this endpoint.") unless parts.size == 2
        {vhost_from_segment(parts[0]), parts[1]}
      rescue URI::Error
        raise InvalidAddress.new("The address #{url} is not valid for this endpoint.")
      end

      # vhost selected by the path of the request itself (the SDK endpoint URL
      # may carry a vhost segment, e.g. http://host:9324/myvhost)
      def self.vhost_from_path(path : String) : String
        parts = path.split('/', remove_empty: true)
        return "/" if parts.empty?
        vhost_from_segment(parts[0])
      end
    end

    module QueueName
      STANDARD = /\A[a-zA-Z0-9_-]{1,80}\z/
      FIFO     = /\A[a-zA-Z0-9_-]{1,75}\.fifo\z/

      def self.valid?(name : String) : Bool
        name.matches?(STANDARD) || name.matches?(FIFO)
      end

      def self.fifo?(name : String) : Bool
        name.ends_with?(".fifo")
      end

      def self.validate!(name : String) : Nil
        return if valid?(name)
        raise InvalidParameterValue.new(
          "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length")
      end
    end
  end
end
