require "./constants"
require "./destination"
require "./source"

module LavinMQ
  module Shovel
    struct ShovelMessage
      getter exchange, routing_key, body, properties, delivery_tag

      def initialize(msg : ::AMQP::Client::DeliverMessage)
        @exchange = msg.exchange
        @routing_key = msg.routing_key
        @body = msg.body_io
        @properties = msg.properties
        @delivery_tag = msg.delivery_tag
      end

      def initialize(msg : String)
        @exchange = ""
        @routing_key = ""
        @body = IO::Memory.new
        @properties = AMQ::Protocol::Properties.new
        @delivery_tag = 0u64
      end
    end

    class Runner
      include SortableJSON
      @state = State::Stopped
      @error : String?
      @message_count : UInt64 = 0
      @retries : Int64 = 0
      RETRY_THRESHOLD =  10
      MAX_DELAY       = 300

      getter name, vhost

      def initialize(@source : Source, @destination : Destination,
                     @name : String, @vhost : VHost, @reconnect_delay : Time::Span = DEFAULT_RECONNECT_DELAY)
        filename = "shovels.#{Digest::SHA1.hexdigest @name}.paused"
        @paused_file_path = File.join(Config.instance.data_dir, filename)
        if File.exists?(@paused_file_path)
          @state = State::Paused
        end
      end

      def state
        @state
      end

      def run
        Log.context.set(name: @name, vhost: @vhost.name)
        loop do
          break if should_stop_loop?
          @state = State::Starting
          @source.start
          @destination.start

          break if should_stop_loop?
          Log.info { "started" }
          @state = State::Running
          @retries = 0
          @source.each do |msg|
            @message_count += 1
            @destination.push(msg, @source)
          end
          break if should_stop_loop? # Don't delete shovel if paused/terminated
          @vhost.delete_parameter("shovel", @name) if @source.delete_after.queue_length?
          break
        rescue ex : ::AMQP::Client::Connection::ClosedException | ::AMQP::Client::Channel::ClosedException | Socket::ConnectError
          break if should_stop_loop?
          @state = State::Error
          # Shoveled queue was deleted
          if ex.message.to_s.starts_with?("404")
            break
          end
          Log.warn { ex.message }
          @error = ex.message
          exponential_reconnect_delay
        rescue ex
          break if should_stop_loop?
          @state = State::Error
          Log.warn { ex.message }
          @error = ex.message
          exponential_reconnect_delay
        end
      ensure
        terminate_if_needed
      end

      private def terminate_if_needed
        terminate if !paused?
      end

      def exponential_reconnect_delay
        @retries += 1
        if @retries > RETRY_THRESHOLD
          sleep Math.min(MAX_DELAY, @reconnect_delay.seconds ** (@retries - RETRY_THRESHOLD)).seconds
        else
          sleep @reconnect_delay
        end
      end

      def details_tuple
        {
          name:          @name,
          vhost:         @vhost.name,
          state:         @state.to_s,
          error:         @error,
          message_count: @message_count,
        }
      end

      def resume
        return unless paused?
        delete_paused_file
        @state = State::Starting
        Log.info { "Resuming shovel #{@name} vhost=#{@vhost.name}" }
        spawn(run, name: "Shovel name=#{@name} vhost=#{@vhost.name}")
      end

      def pause
        return if terminated?
        File.write(@paused_file_path, @name)
        Log.info { "Pausing shovel #{@name} vhost=#{@vhost.name}" }
        @state = State::Paused
        @source.stop
        @destination.stop
        Log.info &.emit("Paused", name: @name, vhost: @vhost.name)
      end

      def delete
        terminate
        delete_paused_file
      end

      # Does not trigger reconnect, but a graceful close
      def terminate
        @state = State::Terminated
        @source.stop
        @destination.stop
        return if terminated?
        Log.info &.emit("Terminated", name: @name, vhost: @vhost.name)
      end

      def delete_paused_file
        FileUtils.rm(@paused_file_path) if File.exists?(@paused_file_path)
      end

      def should_stop_loop?
        terminated? || paused?
      end

      def paused?
        @state.paused?
      end

      def terminated?
        @state.terminated?
      end

      def running?
        @state.running?
      end
    end
  end
end
