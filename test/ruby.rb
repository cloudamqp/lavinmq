require 'bunny'

b = Bunny.new("amqp://guest:guest@localhost/default").start
ch = b.create_channel
q = ch.queue "q2", durable: true
q.subscribe do |d, h, p|
  puts d, h, p
end

10.times do
  q.publish "hej"
end

sleep
b.close
