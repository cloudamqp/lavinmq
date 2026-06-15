require "../../config"

module LavinMQ
  module Clustering
    # Viewstamped Replication coordinator. Replaces etcd for leader election and
    # in-sync tracking: the configured members run the protocol among themselves
    # over a control-connection mesh (see control_mesh.cr).
    module VR
      class Error < Exception; end

      # One configured cluster member: its clustering id (the Int32 exchanged on
      # the wire and stored base-36 in .clustering_id) and its advertised
      # clustering URI (e.g. "tcp://node1:5679").
      record Member, id : Int32, uri : String

      # The static cluster roster. Membership is fixed (operator-configured), so
      # every node computes the same quorum size and the same primary-of-view,
      # with no external coordinator. Ids render base-36 everywhere in LavinMQ
      # (logs, .clustering_id, the old etcd ISR encoding), so the roster parses
      # ids base-36 too.
      class Membership
        # Members sorted ascending by id, so every node iterates them in the same
        # order (used for the deterministic primary-of-view tie-break).
        getter members : Array(Member)
        getter self_id : Int32

        def initialize(members : Array(Member), @self_id : Int32)
          @members = members.sort_by(&.id)
        end

        # The highest op-number committed by a quorum, given each present member's
        # applied op-number (the leader contributes its own latest op, since it
        # has everything it assigned). Returns the quorum-th largest position —
        # every member at or above it forms the committing majority — or nil if
        # fewer than `quorum` members are present, in which case nothing can
        # commit yet (a minority must stall rather than acknowledge). N is the
        # cluster size, so the array is tiny; sorting it is cheap and only
        # happens while a confirm is actually waiting.
        def self.committed_op(quorum : Int32, positions : Array(UInt64)) : UInt64?
          return nil if positions.size < quorum
          positions.sort! { |a, b| b <=> a } # descending; mutates the caller's scratch array
          positions[quorum - 1]
        end

        # Parse a roster string ("id=uri,id=uri,...") and locate this node within
        # it by matching `advertised_uri`. Raises VR::Error on a malformed roster,
        # duplicate ids/uris, or if this node's uri isn't listed.
        def self.parse(roster : String, advertised_uri : String) : Membership
          members = Array(Member).new
          seen_ids = Set(Int32).new
          seen_uris = Set(String).new
          roster.split(',') do |pair|
            pair = pair.strip
            next if pair.empty?
            key, _, uri = pair.partition('=')
            key = key.strip
            uri = uri.strip
            raise Error.new("Malformed clustering member (expected id=uri): #{pair.inspect}") if key.empty? || uri.empty?
            id = key.to_i?(36) || raise Error.new("Invalid clustering member id (base-36 expected): #{key.inspect}")
            raise Error.new("Duplicate clustering member id: #{id.to_s(36)}") unless seen_ids.add?(id)
            raise Error.new("Duplicate clustering member uri: #{uri}") unless seen_uris.add?(uri)
            members << Member.new(id, uri)
          end
          raise Error.new("No clustering members configured") if members.empty?
          self_member = members.find(&.uri.== advertised_uri)
          unless self_member
            raise Error.new("This node's advertised URI #{advertised_uri.inspect} is not in the clustering member roster")
          end
          new(members, self_member.id)
        end

        def self.from_config(config : Config) : Membership
          advertised_uri = config.clustering_advertised_uri ||
                           "tcp://#{System.hostname}:#{config.clustering_port}"
          parse(config.clustering_members, advertised_uri)
        end

        def size : Int32
          @members.size
        end

        # Smallest set that is a strict majority of the roster. A commit or a view
        # change needs this many participating members.
        def quorum : Int32
          size // 2 + 1
        end

        def includes?(id : Int32) : Bool
          @members.any?(&.id.== id)
        end

        def member?(id : Int32) : Member?
          @members.find(&.id.== id)
        end

        def uri_for(id : Int32) : String?
          member?(id).try(&.uri)
        end

        def self_member : Member
          member?(@self_id) || raise Error.new("This node's id #{@self_id.to_s(36)} is not in the roster")
        end

        # The deterministic primary for a view, round-robin over the sorted
        # roster. New-primary selection prefers the most up-to-date node in the
        # view-change quorum (the actual VR rule); this is only the deterministic
        # fallback/tie-break so every node agrees who to address.
        def primary_of(view : UInt64) : Member
          @members[(view % size).to_i]
        end

        # Every member except this node — the peers to which a full control mesh
        # must connect.
        def peers : Array(Member)
          @members.reject(&.id.== @self_id)
        end
      end
    end
  end
end
