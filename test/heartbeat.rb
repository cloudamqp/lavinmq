require 'bunny'

B = Bunny.new("amqp://guest:guest@localhost/%2f", heartbeat: 1)
Thread.new { B.start }
sleep
