#!/usr/bin/env ruby
require 'bunny'

begin
  conn = Bunny.new("amqp://localhost", continuation_timeout: 60_000)
  conn.start

  ch = conn.create_channel
  100_000.times do
    ch.temporary_queue
  end
  puts "Abort to remove qs"
  sleep
ensure
  conn.close if conn.connected?
end
