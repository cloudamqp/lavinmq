require "amqp-client"
require "./perf"

module LavinMQPerf
  class ConnectionCount < Perf
    alias BasicConsumeFrame = AMQP::Client::Frame::Basic::Consume
    @connections = 100
    @channels = 1
    @consumers = 0
    @queue = "connection-count"
    @random_localhost = false
    @done = Channel(Int32).new(100)

    def initialize
      super
      @parser.on("-x count", "--count=number", "Number of connections (default 100)") do |v|
        @connections = v.to_i
      end
      @parser.on("-c channels", "--channels=number", "Number of channels per connection (default 1)") do |v|
        @channels = v.to_i
      end
      @parser.on("-C consumers", "--consumers=number", "Number of consumers per channel (default 0)") do |v|
        @consumers = v.to_i
      end
      @parser.on("-u queue", "--queue=name", "Queue name (default #{@queue})") do |v|
        @queue = v
      end
      @parser.on("-l", "--localhost", "Connect to random localhost 127.0.0.0/16 address") do
        @random_localhost = true
      end
      @parser.on("-k IDLE:COUNT:INTERVAL", "--keepalive=IDLE:COUNT:INTERVAL", "TCP keepalive values") do |v|
        @uri.query_params["tcp_keepalive"] = v
      end
      puts "FD limit: #{System.maximize_fd_limit}"
    end

    private def connect(i)
      c = client.connect
      @channels.times do |j|
        ch = c.channel
        @consumers.times do |k|
          ch.queue(@queue) if j == k == 0
          # Send raw frame, no wait, no fiber
          c.write BasicConsumeFrame.new(ch.id, 0_u16, "", "", false, true, false, true, AMQP::Client::Arguments.new)
        end
      end
      @done.send i
    end

    def run
      super
      count = 0
      loop do
        @connections.times.each_slice(100) do |slice|
          start = Time.monotonic
          slice.each do |i|
            spawn connect(i)
          end
          slice.each do |_i|
            @done.receive
            print '.'
          end
          stop = Time.monotonic
          puts " #{(stop - start).total_milliseconds.round}ms"
        end
        puts
        print "#{count += @connections} connections "
        print "#{count * @channels} channels "
        print "#{count * @channels * @consumers} consumers. "
        puts "Using #{rss.humanize_bytes} memory."
        puts "Press enter to do add #{@connections} connections or ctrl-c to abort"
        gets
      end
    end

    private def client : AMQP::Client
      client = @client ||= AMQP::Client.new(@uri)
      client.host = "127.0.#{Random.rand(UInt8)}.#{Random.rand(UInt8)}" if @random_localhost
      client
    end
  end
end
