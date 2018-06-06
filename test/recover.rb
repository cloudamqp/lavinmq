require 'bunny'

B = Bunny.new("amqp://guest:guest@localhost/%2f")
B.start
ch = B.create_channel
x = ch.fanout("amq.fanout")
q = ch.queue("", exclusive: true).bind(x)
print "Restart server then press enter ..."
gets
puts "Recovered? #{B.open?}"
x.publish("")
