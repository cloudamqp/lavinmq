require 'bunny'

i = 0
Thread.new do
  loop do
    i = 0
    sleep 1
    puts "Consumed: #{i} msgs/s"
  end
end

Bunny.run("amqp://guest:guest@localhost/default") do |b|
  ch = b.create_channel
  q1 = ch.queue "f1", durable: true
  q1.bind "amq.fanout"
  q1.subscribe(block: true, manual_ack: true) do |d, _h, _p|
    i += 1
    d.ack
  end
end
