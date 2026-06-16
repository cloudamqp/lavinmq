require "uri"

module LavinMQ
  module Clustering
    class StaticMembers
      getter members : Hash(Int32, URI)

      def self.parse(raw : String) : self
        members = Hash(Int32, URI).new
        raw.split(',') do |entry|
          entry = entry.strip
          next if entry.empty?

          id_raw, uri_raw = entry.split('=', 2)
          id = id_raw.to_i32
          raise Error.new("Duplicate clustering member id #{id}") if members.has_key?(id)

          uri = URI.parse(uri_raw)
          raise Error.new("Clustering member #{id} is missing a host: #{uri_raw}") unless uri.hostname
          members[id] = uri
        rescue ex : IndexError | ArgumentError
          raise Error.new("Invalid clustering member entry '#{entry}', expected id=uri")
        end
        raise Error.new("clustering.members must not be empty") if members.empty?
        new(members)
      end

      def initialize(@members : Hash(Int32, URI))
      end

      def ids : Array(Int32)
        @members.keys.sort!
      end

      def size : Int32
        @members.size
      end

      def quorum_size : Int32
        (size // 2) + 1
      end

      def includes?(id : Int32) : Bool
        @members.has_key?(id)
      end

      def uri(id : Int32) : URI
        @members[id]
      end

      def primary_id(view : Int64) : Int32
        sorted = ids
        sorted[(view % sorted.size).to_i]
      end

      def derive_node_id(advertised_uri : String?) : Int32
        raise Error.new("clustering.node_id is required when clustering.advertised_uri is not set") unless advertised_uri
        parsed = URI.parse(advertised_uri)
        matches = @members.select { |_id, uri| equivalent_uri?(uri, parsed) }.keys
        case matches.size
        when 1 then matches.first
        when 0 then raise Error.new("clustering.advertised_uri #{advertised_uri} does not match clustering.members")
        else        raise Error.new("clustering.advertised_uri #{advertised_uri} matches multiple clustering.members")
        end
      end

      private def equivalent_uri?(a : URI, b : URI) : Bool
        a.scheme == b.scheme &&
          a.hostname == b.hostname &&
          (a.port || default_port(a.scheme)) == (b.port || default_port(b.scheme))
      end

      private def default_port(scheme : String?) : Int32?
        case scheme
        when "tcp", "lavinmq" then 5679
        end
      end

      class Error < Exception; end
    end
  end
end
