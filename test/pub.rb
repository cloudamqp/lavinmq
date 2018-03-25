require 'bunny'

j = 0
Thread.new do
  loop do
    j = 0
    sleep 1
    puts "Published: #{j} msgs/s"
  end
end

Bunny.run("amqp://guest:guest@localhost/default") do |b|
  ch = b.create_channel
  e = ch.exchange("amq.fanout", type: :fanout)
  ch.queue("f1", durable: true).bind(e)
  msg = "a" * 1024
  loop do
    e.publish msg
    j += 1
  end
end
