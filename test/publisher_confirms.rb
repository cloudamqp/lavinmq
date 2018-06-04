#!/usr/bin/env ruby
require "bunny"

puts "=> Using publisher confirms"
puts

conn = Bunny.new
conn.start

ch   = conn.create_channel
x    = ch.fanout("amq.fanout")
q    = ch.queue("", exclusive: true).bind(x)

# Put channel in confirmation mode
ch.confirm_select

1000.times do
  x.publish("")
end

# Block until all messages have been confirmed
success = ch.wait_for_confirms

unless success
  ch.nacked_set.each do |n|
    puts "#{n} NACKed"
  end
end

sleep 0.2
puts "Processed all published messages. #{q.name} now has #{q.message_count} messages."

sleep 0.5
puts "Closing..."
conn.close
