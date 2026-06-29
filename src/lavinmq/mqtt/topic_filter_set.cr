require "./subscription_tree"

module LavinMQ
  module MQTT
    # A compiled set of MQTT topic filters used for one verb (read or write).
    # `matches?` is the hot path (per delivered/published message) and uses the
    # zero-allocation SubscriptionTree. `overlaps?` is the cold path (per
    # SUBSCRIBE) and may allocate.
    class TopicFilterSet
      def initialize
        @tree = SubscriptionTree(Nil).new
        @filters = [] of String
      end

      def add(filter : String) : Nil
        @filters << filter
        @tree.subscribe(filter, nil, 0u8)
      end

      def empty? : Bool
        @filters.empty?
      end

      # Hot path: does any filter match this concrete topic?
      def matches?(topic : String) : Bool
        @tree.covers?(topic)
      end

      # Cold path: does the subscription filter share any concrete topic with
      # any rule in this set?
      def overlaps?(filter : String) : Bool
        @filters.any? { |f| TopicFilterSet.filters_overlap?(filter, f) }
      end

      def self.expand(pattern : String, username : String) : String
        pattern.gsub("{username}", username)
      end

      # True if MQTT filters a and b can both match at least one concrete topic.
      def self.filters_overlap?(a : String, b : String) : Bool
        overlap?(a.split('/'), b.split('/'), 0, 0)
      end

      # True if segment seg is a multi-level wildcard consuming all remaining levels.
      private def self.hash_wildcard?(seg : String) : Bool
        seg == "#"
      end

      # True if two segments at the same level are compatible (either is '+' or they are equal).
      private def self.segments_match?(ai : String, bj : String) : Bool
        ai == "+" || bj == "+" || ai == bj
      end

      private def self.overlap?(a : Array(String), b : Array(String), i : Int32, j : Int32) : Bool
        loop do
          a_done = i >= a.size
          b_done = j >= b.size
          return true if a_done && b_done
          # '#' matches the remainder, including zero further levels.
          return true if !a_done && hash_wildcard?(a[i])
          return true if !b_done && hash_wildcard?(b[j])
          return false if a_done || b_done
          return false unless segments_match?(a[i], b[j])
          i += 1
          j += 1
        end
      end
    end
  end
end
