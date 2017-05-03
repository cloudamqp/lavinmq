require "amqp"

abort "Usage: #{PROGRAM_NAME} <ip:port> <msgs>" if ARGV.size != 2
url, msgs = ARGV
host, port = url.split(":")
puts "Publishing #{msgs} msgs"
conf = AMQP::Config.new(host, port.to_i)
msgs.to_i.times do |idx|
  AMQP::Connection.start(conf) do |conn|
    conn.on_close do |code, msg|
      puts "CONNECTION CLOSED: #{code} - #{msg}"
    end

    channel = conn.channel
    channel.on_close do |code, msg|
      puts "CHANNEL CLOSED: #{code} - #{msg}"
    end

    exchange = channel.topic("amq.topic", durable: true)
    queue = channel.queue("test")
    #queue.bind(exchange, "r.*")
    #queue.subscribe do |msg|
    #  puts "Received msg (1): #{msg.key} #{String.new(msg.body)}"
    #  msg.ack
    #end
    #queue.subscribe do |msg|
    #  puts "Received msg (2): #{msg.properties}"
    #  msg.ack
    #end

    msg = AMQP::Message.new("test message: #{idx+1}")
    exchange.publish(msg, "r.k")
  end
end
