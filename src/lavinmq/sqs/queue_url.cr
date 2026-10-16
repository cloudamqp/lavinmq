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
        return "/" if segment == DEFAULT_ACCOUNT_ID
        decoded = segment.includes?('%') ? URI.decode(segment) : segment
        decoded.matches?(ACCOUNT_ID_PATTERN) ? "/" : decoded
      end

      # Queue names minted over SQS need no escaping, but queues declared over
      # AMQP may contain any character, e.g. `/` or a space.
      def self.build(base_url : String, vhost : String, queue_name : String) : String
        "#{base_url}/#{vhost_segment(vhost)}/#{URI.encode_path_segment(queue_name)}"
      end

      # Parses scheme://host[:port]/<vhost-segment>/<queue>[/][?query][#fragment]
      # with index scans and returns {vhost, queue_name}. Every request carries
      # a queue URL, so building a URI and splitting its path showed up in
      # profiles. The name is decoded after splitting, so an AMQP queue name
      # containing an encoded `/` stays one segment.
      def self.parse(url : String) : Tuple(String, String)
        path_start, path_end = path_bounds(url)
        separator = url.index('/', path_start + 1)
        if separator.nil? || separator >= path_end || separator == path_start + 1 || separator + 1 == path_end
          raise invalid_address(url)
        end
        raise invalid_address(url) if url.index('/', separator + 1).try(&.< path_end)
        vhost = vhost_from_segment(url.byte_slice(path_start + 1, separator - path_start - 1))
        name = url.byte_slice(separator + 1, path_end - separator - 1)
        name = URI.decode(name) if name.includes?('%')
        {vhost, name}
      end

      # Byte offsets of the path: from its leading `/` up to (not including)
      # the query, fragment or a trailing `/`
      private def self.path_bounds(url : String) : Tuple(Int32, Int32)
        scheme_end = url.index("://")
        raise invalid_address(url) if scheme_end.nil? || scheme_end.zero?
        host_start = scheme_end + 3
        path_start = url.index('/', host_start)
        raise invalid_address(url) if path_start.nil? || path_start == host_start
        path_end = path_end(url, path_start)
        path_end -= 1 if path_end > path_start + 1 && url.byte_at(path_end - 1) == '/'.ord
        {path_start, path_end}
      end

      private def self.path_end(url : String, from : Int32) : Int32
        i = from
        while i < url.bytesize
          byte = url.byte_at(i)
          return i if byte == '?'.ord || byte == '#'.ord
          i += 1
        end
        url.bytesize
      end

      private def self.invalid_address(url : String) : InvalidAddress
        InvalidAddress.new("The address #{url} is not valid for this endpoint.")
      end

      # vhost selected by the path of the request itself (the SDK endpoint URL
      # may carry a vhost segment, e.g. http://host:9324/myvhost)
      def self.vhost_from_path(path : String) : String
        return "/" if path == "/" || path.empty?
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
