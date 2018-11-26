#!/usr/bin/env ruby
require 'bunny'

puts "Usage: create_qs number_of_queues" && exit unless ARGV.size == 1

begin
  conn = Bunny.new("amqp://localhost", continuation_timeout: 60_000)
  conn.start

  ch = conn.create_channel
  ARGV[0].to_i.times do
    ch.temporary_queue
  end
  puts "Abort to remove qs"
  sleep
ensure
  conn.close if conn.connected?
end
