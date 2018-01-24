require 'bunny'

i =  0
Thread.new do
  loop do
    i = 0
    sleep 1
    puts "New connections: #{i} msgs/s"
  end
end

loop do
  b = Bunny.new("amqp://guest:guest@localhost/default").start
  b.close
  i += 1
end
sleep
