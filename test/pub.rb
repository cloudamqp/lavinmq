require 'bunny'

j = 0
Thread.new do
  loop do
    j = 0
    sleep 1
    puts "Published: #{j} msgs/s"
  end
end

b = Bunny.new("amqp://guest:guest@localhost/default").start
ch2 = b.create_channel
e = ch2.exchange("amq.fanout", type: :fanout)
q = ch2.queue("f1", durable: true)
q.bind(e)
msg = "a" * 1024
loop do
  e.publish msg
  j += 1
end
sleep
