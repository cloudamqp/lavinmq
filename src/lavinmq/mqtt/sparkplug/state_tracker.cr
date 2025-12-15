module LavinMQ
  module MQTT
    module Sparkplug
      struct EdgeNodeState
        property? online : Bool
        property last_birth_timestamp : Int64
        property last_death_timestamp : Int64?

        def initialize(@online = false, @last_birth_timestamp = 0_i64, @last_death_timestamp = nil)
        end
      end

      class StateTracker
        def initialize
          @states = Hash(String, EdgeNodeState).new
          @lock = Mutex.new
        end

        # Mark edge node as online (NBIRTH received)
        def track_birth(group_id : String, edge_node_id : String) : Nil
          key = make_key(group_id, edge_node_id)
          timestamp = RoughTime.unix_ms

          @lock.synchronize do
            if state = @states[key]?
              state.online = true
              state.last_birth_timestamp = timestamp
              @states[key] = state
            else
              @states[key] = EdgeNodeState.new(
                online: true,
                last_birth_timestamp: timestamp,
                last_death_timestamp: nil
              )
            end
          end
        end

        # Mark edge node as offline (NDEATH received)
        def track_death(group_id : String, edge_node_id : String) : Nil
          key = make_key(group_id, edge_node_id)
          timestamp = RoughTime.unix_ms

          @lock.synchronize do
            if state = @states[key]?
              state.online = false
              state.last_death_timestamp = timestamp
              @states[key] = state
            else
              # Edge node died without ever sending BIRTH (shouldn't happen, but handle it)
              @states[key] = EdgeNodeState.new(
                online: false,
                last_birth_timestamp: 0_i64,
                last_death_timestamp: timestamp
              )
            end
          end
        end

        # Check if edge node is currently online
        def online?(group_id : String, edge_node_id : String) : Bool
          key = make_key(group_id, edge_node_id)

          @lock.synchronize do
            @states[key]?.try(&.online) || false
          end
        end

        # Get state for an edge node
        def state(group_id : String, edge_node_id : String) : EdgeNodeState?
          key = make_key(group_id, edge_node_id)

          @lock.synchronize do
            @states[key]?
          end
        end

        # Iterate over all edge node states
        def each(& : String, EdgeNodeState ->)
          @lock.synchronize do
            @states.each do |key, state|
              yield key, state
            end
          end
        end

        # Get count of online edge nodes
        def online_count : Int32
          count = 0
          @lock.synchronize do
            @states.each_value do |state|
              count += 1 if state.online
            end
          end
          count
        end

        # Get total count of tracked edge nodes
        def total_count : Int32
          @lock.synchronize do
            @states.size
          end
        end

        # Remove edge node from tracking (optional cleanup)
        def remove(group_id : String, edge_node_id : String) : Nil
          key = make_key(group_id, edge_node_id)

          @lock.synchronize do
            @states.delete(key)
          end
        end

        # Clear all tracked states
        def clear : Nil
          @lock.synchronize do
            @states.clear
          end
        end

        private def make_key(group_id : String, edge_node_id : String) : String
          "#{group_id}/#{edge_node_id}"
        end
      end
    end
  end
end
