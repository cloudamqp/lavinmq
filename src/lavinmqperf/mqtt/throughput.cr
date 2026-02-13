require "../../lavinmq/mqtt/protocol"
require "json"
require "wait_group"
require "../perf"
require "atomic"
require "openssl"

module LavinMQPerf
  module MQTT
    class Throughput < Perf
      @publishers = 1
      @consumers = 1
      @size = 16
      @verify = false
      @topic = "perf-test"
      @qos = 0
      @rate = 0
      @consume_rate = 0
      @quiet = false
      @json_output = false
      @timeout = Time::Span.zero
      @pmessages = 0
      @cmessages = 0
      @random_bodies = false
      @random = Random.new
      @retain = false
      @clean_session = false
      @tls_no_verify = false
      @uri = URI.parse("mqtt://localhost:1883")

      def initialize(io : IO = STDOUT)
        super(io)
        @parser.on("-x publishers", "--publishers=number", "Number of publishers (default 1)") do |v|
          @publishers = v.to_i
        end
        @parser.on("-y consumers", "--consumers=number", "Number of consumers (default 1)") do |v|
          @consumers = v.to_i
        end
        @parser.on("-s msgsize", "--size=bytes", "Size of each message (default 16 bytes)") do |v|
          @size = v.to_i
        end
        @parser.on("-V", "--verify", "Verify the message body") do
          @verify = true
        end
        @parser.on("-q qos", "--qos=level", "QoS level (0 or 1)") do |v|
          @qos = v.to_i
        end
        @parser.on("-t topic", "--topic=name", "Topic name (default perf-test)") do |v|
          @topic = v
        end
        @parser.on("-r pub-rate", "--rate=number", "Max publish rate (default 0)") do |v|
          @rate = v.to_i
        end
        @parser.on("-R consumer-rate", "--consumer-rate=number", "Max consume rate (default 0)") do |v|
          @consume_rate = v.to_i
        end
        @parser.on("-j", "--json", "Output result as JSON") do
          @json_output = true
        end
        @parser.on("-z seconds", "--time=seconds", "Only run for X seconds") do |v|
          @timeout = Time::Span.new(seconds: v.to_i)
        end
        @parser.on("-q", "--quiet", "Quiet, only print the summary") do
          @quiet = true
        end
        @parser.on("-C messages", "--pmessages=messages", "Publish max X number of messages") do |v|
          @pmessages = v.to_i
        end
        @parser.on("-D messages", "--cmessages=messages", "Consume max X number of messages") do |v|
          @cmessages = v.to_i
        end
        @parser.on("--random-bodies", "Each message body is random") do
          @random_bodies = true
        end
        @parser.on("--retain", "Set retain flag on published messages") do
          @retain = true
        end
        @parser.on("--clean-session", "Use clean session") do
          @clean_session = true
        end
        @parser.on("--tls-no-verify", "Skip TLS certificate verification") do
          @tls_no_verify = true
        end
        @parser.on("-u uri", "--uri=uri", "MQTT broker URI (default mqtt://localhost:1883)") do |v|
          @uri = URI.parse(v)
        end
      end

      @pubs = Atomic(UInt64).new(0_u64)
      @consumes = Atomic(UInt64).new(0_u64)
      @stopped = false

      private def create_client(id : Int32, role : String) : {IO, LavinMQ::MQTT::IO}
        if @uri.host == @uri.port == nil
          @uri = URI.parse("mqtt://#{@uri.scheme}:#{@uri.path}")
        end

        tls = @uri.scheme == "mqtts"
        host = @uri.host || "localhost"
        port = @uri.port || (tls ? 8883 : 1883)
        user = @uri.user || "guest"
        password = @uri.password || "guest"

        socket = connect_socket(host, port, tls)
        io = LavinMQ::MQTT::IO.new(socket)

        client_id = "#{role}-#{id}"
        connect_packet = LavinMQ::MQTT::Connect.new(
          client_id: client_id,
          clean_session: @clean_session,
          keepalive: 0,
          username: user,
          password: password.to_slice,
          will: nil
        )

        connect_packet.to_io(io)
        io.flush

        response = LavinMQ::MQTT::Packet.from_io(io)
        unless response.is_a?(LavinMQ::MQTT::Connack) &&
               response.as(LavinMQ::MQTT::Connack).return_code == LavinMQ::MQTT::Connack::ReturnCode::Accepted
          socket.try &.close rescue nil
          raise "Failed to connect: #{response.inspect}"
        end

        @io.puts "Connected to broker with --uri=#{@uri}"
        {socket, io}
      end

      private def connect_socket(host : String, port : Int32, tls : Bool) : IO
        tcp_socket = TCPSocket.new(host, port)
        tcp_socket.keepalive = true
        tcp_socket.tcp_nodelay = false
        tcp_socket.sync = false

        if tls
          ssl_context = OpenSSL::SSL::Context::Client.new
          ssl_context.verify_mode = OpenSSL::SSL::VerifyMode::NONE if @tls_no_verify
          begin
            ssl_socket = OpenSSL::SSL::Socket::Client.new(tcp_socket, context: ssl_context, sync_close: true, hostname: host)
          rescue ex
            tcp_socket.close rescue nil
            raise ex
          end
          ssl_socket.sync = false
          return ssl_socket
        end

        tcp_socket
      end

      def run(args = ARGV)
        super(args)
        mt = Fiber::ExecutionContext::Parallel.new("Consumer", maximum: System.cpu_count.to_i)
        mt2 = Fiber::ExecutionContext::Parallel.new("Publisher", maximum: System.cpu_count.to_i)
        done = WaitGroup.new(@consumers + @publishers)
        connected = WaitGroup.new(@consumers + @publishers)
        @consumers.times do |i|
          mt.spawn { rerun_on_exception(done) { consume(i, connected) } }
        end

        sleep 1.second # Give consumers time to connect

        @publishers.times do |i|
          mt2.spawn { rerun_on_exception(done) { pub(i, connected) } }
        end

        if @timeout != Time::Span.zero
          spawn do
            sleep @timeout
            @stopped = true
          end
        end

        connected.wait # wait for all clients to connect
        start = Time.instant
        Process.on_terminate do
          abort "Aborting" if @stopped
          @stopped = true
          summary(start)
          exit 0
        end

        spawn do
          done.wait
          @stopped = true
        end

        loop do
          break if @stopped
          pubs_last = @pubs.get(:relaxed)
          consumes_last = @consumes.get(:relaxed)
          sleep 1.seconds
          unless @quiet
            @io.puts "Publish rate: #{@pubs.get(:relaxed) - pubs_last} msgs/s Consume rate: #{@consumes.get(:relaxed) - consumes_last} msgs/s"
          end
        end
        summary(start)
      end

      private def summary(start : Time::Instant)
        stop = Time.instant
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs.get(:relaxed) / elapsed).round(1)
        avg_consume = (@consumes.get(:relaxed) / elapsed).round(1)
        puts
        if @json_output
          JSON.build(@io) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "avg_pub_rate", avg_pub
              json.field "avg_consume_rate", avg_consume
              json.field "total_published", @pubs.get(:relaxed)
              json.field "total_consumed", @consumes.get(:relaxed)
            end
          end
          puts
        else
          @io.puts "Summary:"
          @io.puts "Average publish rate: #{avg_pub} msgs/s"
          @io.puts "Average consume rate: #{avg_consume} msgs/s"
          @io.puts "Total published: #{@pubs.get(:relaxed)}"
          @io.puts "Total consumed: #{@consumes.get(:relaxed)}"
        end
      end

      private def pub(id : Int32, connected) # ameba:disable Metrics/CyclomaticComplexity
        socket, io = create_client(id, "publisher")

        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Fiber.yield

        start = Time.instant
        pubs_this_second = 0
        packet_id_generator = (1_u16..).each
        wait_until_all_are_connected(connected)
        until @stopped
          @random.random_bytes(data) if @random_bodies
          packet_id = @qos > 0 ? packet_id_generator.next.as(UInt16) : nil

          publish = LavinMQ::MQTT::Publish.new(
            topic: @topic,
            payload: data,
            packet_id: packet_id,
            qos: @qos.to_u8,
            retain: @retain,
            dup: false
          )
          publish.to_io(io)
          io.flush

          if @qos > 0
            ack = LavinMQ::MQTT::Packet.from_io(io)
            unless ack.is_a?(LavinMQ::MQTT::PubAck)
              raise "Expected PUBACK but got #{ack.inspect}"
            end
          end

          pubs = @pubs.add(1, :relaxed) + 1
          break if @pmessages > 0 && pubs >= @pmessages

          if !@rate.zero?
            pubs_this_second += 1
            if pubs_this_second >= @rate
              until_next_second = (start + 1.seconds) - Time.instant
              if until_next_second > Time::Span.zero
                sleep until_next_second
              end
              start = Time.instant
              pubs_this_second = 0
            end
          end
        end
        LavinMQ::MQTT::Disconnect.new.to_io(io) if socket && !socket.closed?
      ensure
        socket.try &.close rescue nil
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def consume(id : Int32, connected)
        socket, io = create_client(id, "consumer")
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Fiber.yield

        start = Time.instant
        consumes_this_second = 0

        topic_filter = LavinMQ::MQTT::Subscribe::TopicFilter.new(@topic, @qos.to_u8)
        LavinMQ::MQTT::Subscribe.new([topic_filter], packet_id: 1_u16).to_io(io)
        io.flush

        suback = LavinMQ::MQTT::Packet.from_io(io)
        unless suback.is_a?(LavinMQ::MQTT::SubAck)
          raise "Expected SUBACK but got #{suback.inspect}"
        end
        individual_consumes = 0
        wait_until_all_are_connected(connected)
        until @stopped
          begin
            packet = LavinMQ::MQTT::Packet.from_io(io)
            case packet
            when LavinMQ::MQTT::Publish
              consumes = @consumes.add(1, :relaxed) + 1
              individual_consumes += 1

              if @verify
                raise "Invalid data: #{packet.payload}" if packet.payload != data
              end

              if packet.qos > 0 && (packet_id = packet.packet_id)
                LavinMQ::MQTT::PubAck.new(packet_id).to_io(io)
                io.flush
              end

              if @cmessages > 0 && consumes >= @cmessages
                break
              end

              if !@consume_rate.zero?
                consumes_this_second += 1
                if consumes_this_second >= @consume_rate
                  until_next_second = (start + 1.seconds) - Time.instant
                  if until_next_second > Time::Span.zero
                    sleep until_next_second
                  end
                  start = Time.instant
                  consumes_this_second = 0
                end
              else
                Fiber.yield if individual_consumes % (128 * 1024) == 0
              end
            when LavinMQ::MQTT::PingReq
              LavinMQ::MQTT::PingResp.new.to_io(io)
              io.flush
            end
          rescue ex : IO::TimeoutError
            @io.puts ex
            next
          end
        end
        if socket && !socket.closed?
          LavinMQ::MQTT::Disconnect.new.to_io(io)
          io.flush
        end
      ensure
        socket.try &.close rescue nil
      end

      private def rerun_on_exception(done, &)
        loop do
          break yield
        rescue ex
          @io.puts ex.message
          sleep 1.seconds
        end
      ensure
        done.done
      end

      # Announce that the client is connected
      # and then wait for all other clients to be connected too
      private def wait_until_all_are_connected(connected)
        connected.done
        connected.wait
      rescue
        # when we reconnect a broker the waitgroup will have a negative counter
      end
    end
  end
end
