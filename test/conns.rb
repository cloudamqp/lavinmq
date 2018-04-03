require 'bunny'

scheme = "amqp"
scheme = "amqps" if ARGV.shift == "--tls"

i = 0
Thread.new do
  loop do
    i = 0
    sleep 1
    puts "New connections: #{i} msgs/s"
  end
end

loop do
  b = Bunny.new("#{scheme}://guest:guest@localhost/default", verify_peer: false).start
  b.close
  i += 1
end
sleep
