require "../../lavinmq/mqtt/protocol"
require "json"
require "wait_group"
require "../perf"
require "atomic"

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
      @retain = false
      @clean_session = false
      @uri = URI.parse("mqtt://localhost:1883")

      def initialize
        super
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
        @parser.on("-u uri", "--uri=uri", "MQTT broker URI (default mqtt://localhost:1883)") do |v|
          @uri = URI.parse(v)
        end
      end

      @pubs = Atomic(UInt64).new(0_u64)
      @consumes = Atomic(UInt64).new(0_u64)
      @stopped = false

      private def create_client(id : Int32, role : String) : {TCPSocket, LavinMQ::MQTT::IO}
        if @uri.host == @uri.port == nil
          @uri = URI.parse("mqtt://#{@uri.scheme}:#{@uri.path}")
        end

        host = @uri.host || "localhost"
        port = @uri.port || 1883
        user = @uri.user || "guest"
        password = @uri.password || "guest"

        socket = TCPSocket.new(host, port)
        socket.keepalive = true
        socket.tcp_nodelay = false
        socket.sync = true
        io = LavinMQ::MQTT::IO.new(socket)

        client_id = "#{role}-#{id}"
        connect_packet = LavinMQ::MQTT::Connect.new(
          client_id: client_id,
          clean_session: @clean_session,
          keepalive: 30_u16,
          username: user,
          password: password.to_slice,
          will: nil
        )

        connect_packet.to_io(io)

        response = LavinMQ::MQTT::Packet.from_io(io)
        unless response.is_a?(LavinMQ::MQTT::Connack) &&
               response.as(LavinMQ::MQTT::Connack).return_code == LavinMQ::MQTT::Connack::ReturnCode::Accepted
          socket.try &.close rescue nil
          raise "Failed to connect: #{response.inspect}"
        end

        puts "Connected to broker with --uri=#{@uri}"
        {socket, io}
      end

      def run
        super
        mt = Fiber::ExecutionContext::MultiThreaded.new("Clients", maximum: System.cpu_count.to_i)
        done = WaitGroup.new(@consumers + @publishers)
        @consumers.times do |i|
          mt.spawn { rerun_on_exception(done) { consume(i) } }
        end

        sleep 1.second # Give consumers time to connect

        @publishers.times do |i|
          mt.spawn { rerun_on_exception(done) { pub(i) } }
        end

        if @timeout != Time::Span.zero
          spawn do
            sleep @timeout
            @stopped = true
          end
        end

        Fiber.yield # wait for all clients to connect
        start = Time.monotonic
        Signal::INT.trap do
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
          pubs_last = @pubs
          consumes_last = @consumes
          sleep 1.seconds
          unless @quiet
            puts "Publish rate: #{@pubs.get - pubs_last.get} msgs/s Consume rate: #{@consumes.get - consumes_last.get} msgs/s"
          end
        end
        summary(start)
      end

      private def summary(start : Time::Span)
        stop = Time.monotonic
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs.get / elapsed).round(1)
        avg_consume = (@consumes.get / elapsed).round(1)
        puts
        if @json_output
          JSON.build(STDOUT) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "avg_pub_rate", avg_pub
              json.field "avg_consume_rate", avg_consume
              json.field "total_published", @pubs.get
              json.field "total_consumed", @consumes.get
            end
          end
          puts
        else
          puts "Summary:"
          puts "Average publish rate: #{avg_pub} msgs/s"
          puts "Average consume rate: #{avg_consume} msgs/s"
          puts "Total published: #{@pubs.get}"
          puts "Total consumed: #{@consumes.get}"
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def pub(id : Int32)
        socket, io = create_client(id, "publisher")

        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Fiber.yield

        start = Time.monotonic
        pubs_this_second = 0
        packet_id_generator = (1_u16..).each

        until @stopped
          Random::DEFAULT.random_bytes(data) if @random_bodies
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

          if @qos > 0
            ack = LavinMQ::MQTT::Packet.from_io(io)
            unless ack.is_a?(LavinMQ::MQTT::PubAck)
              raise "Expected PUBACK but got #{ack.inspect}"
            end
          end

          @pubs.add(1)
          break if @pmessages > 0 && @pubs.get >= @pmessages

          if !@rate.zero?
            pubs_this_second += 1
            if pubs_this_second >= @rate
              until_next_second = (start + 1.seconds) - Time.monotonic
              if until_next_second > Time::Span.zero
                sleep until_next_second
              end
              start = Time.monotonic
              pubs_this_second = 0
            end
          end
        end
        LavinMQ::MQTT::Disconnect.new.to_io(io) if socket && !socket.closed?
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def consume(id : Int32)
        socket, io = create_client(id, "consumer")
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Fiber.yield

        start = Time.monotonic
        consumes_this_second = 0

        topic_filter = LavinMQ::MQTT::Subscribe::TopicFilter.new(@topic, @qos.to_u8)
        LavinMQ::MQTT::Subscribe.new([topic_filter], packet_id: 1_u16).to_io(io)

        suback = LavinMQ::MQTT::Packet.from_io(io)
        unless suback.is_a?(LavinMQ::MQTT::SubAck)
          raise "Expected SUBACK but got #{suback.inspect}"
        end

        done_channel = Channel(Nil).new(1)
        until @stopped
          begin
            packet = LavinMQ::MQTT::Packet.from_io(io)
            case packet
            when LavinMQ::MQTT::Publish
              publish = packet.as(LavinMQ::MQTT::Publish)

              @consumes.add(1)

              if @verify
                raise "Invalid data: #{publish.payload}" if publish.payload != data
              end

              if publish.qos > 0 && (packet_id = publish.packet_id)
                LavinMQ::MQTT::PubAck.new(packet_id).to_io(io)
              end

              if @cmessages > 0 && @consumes.get >= @cmessages
                done_channel.send(nil)
                break
              end

              if !@consume_rate.zero?
                consumes_this_second += 1
                if consumes_this_second >= @consume_rate
                  until_next_second = (start + 1.seconds) - Time.monotonic
                  if until_next_second > Time::Span.zero
                    sleep until_next_second
                  end
                  start = Time.monotonic
                  consumes_this_second = 0
                end
              else
                Fiber.yield if @consumes.get % (128 * 1024) == 0
              end
            when LavinMQ::MQTT::PingReq
              LavinMQ::MQTT::PingResp.new.to_io(io)
            end
          rescue ex : IO::TimeoutError
            next
          end
        end
        done_channel.receive
        LavinMQ::MQTT::Disconnect.new.to_io(io) if socket && !socket.closed?
      end

      private def rerun_on_exception(done, &)
        loop do
          break yield
        rescue ex
          puts ex.message
          sleep 1.seconds
        end
      ensure
        done.done
      end
    end
  end
end
