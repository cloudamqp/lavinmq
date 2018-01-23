require 'bunny'


i = j = 0
Thread.new do
  loop do
    i = j = 0
    sleep 1
    puts "Consumed: #{i} msgs/s"
    puts "Published: #{j} msgs/s"
  end
end

b = Bunny.new("amqp://guest:guest@localhost/default").start
ch = b.create_channel
q1 = ch.queue "q1", durable: true, auto_delete: false
q1.bind "amq.fanout"
q1.subscribe do |d, h, p|
  i += 1
end

ch2 = b.create_channel
e = ch2.exchange("amq.fanout", type: :fanout)
msg = " "
loop do
  e.publish msg
  j += 1
end
sleep
