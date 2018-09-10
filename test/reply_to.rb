require 'bunny'

Thread.abort_on_exception = true
q = "test.direct.reply-to"

# Server
s = Thread.new do
  Bunny.run("amqp://guest:guest@localhost/%2f") do |b|
    ch = b.create_channel
    ch.queue(q).subscribe do |_delivery, headers, body|
      correlation_id = headers[:correlation_id]
      print "Server sending direct reply with correlation_id #{correlation_id} and body #{body}"\
          " to #{headers[:reply_to]}\n"
      ch.basic_publish body, "", headers[:reply_to]
      next
    end
    puts "Server subscribing to #{q} on ch=#{ch.id} "\
         "consumers=#{ch.consumers.keys.join(', ')}"
    loop { sleep 1 }
  end
end

sleep 1

# Client
c = Thread.new do
  Bunny.run("amqp://guest:guest@localhost/%2f") do |b|
    waiting = false
    ch = b.create_channel
    ch.queue("amq.rabbitmq.reply-to").subscribe do |_delivery, headers, body|
      correlation_id = headers[:correlation_id]
      puts "Client got direct reply with correlation_id #{correlation_id} and body #{body}"
      waiting = false
    end
    puts "Client subscribing to amq.rabbitmq.reply-to on ch=#{ch.id} "\
          "consumers=#{ch.consumers.keys.join(', ')}"
    loop do
      next if waiting
      print "Enter to send rpc ..."
      gets
      puts "Client sending to queue test with reply_to direct"
      ch.basic_publish "test", "", q, {
        reply_to: "amq.rabbitmq.reply-to",
        expiration: 3_000
      }
      waiting = true
    end
  end
end

c.join
s.join
