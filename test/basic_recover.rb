require 'bunny'
B = Bunny.new('amqp://localhost')
B.start

def publish_100_msgs(qname)
  pub_ch = B.create_channel
  100.times { |i| pub_ch.direct('').publish("#{i}", routing_key: qname) }
end
def get(qname, ch)
  delivery, _headers, body = ch.basic_get(qname, manual_ack: true)
  ack = yield delivery.delivery_tag, body
  ch.acknowledge(delivery.delivery_tag, false) if ack
end
count = 0
ch = B.create_channel(nil, 1)
ch.queue('test_get1', durable: false, auto_delete: true)
publish_100_msgs('test_get1')
ch.prefetch 100
ack = false
begin
  150.times do |i|
    if i == 50
      ch.basic_recover(true)
      ack = true
    end
    get('test_get1', ch) do |dt, msg|
      puts "queue=test_get1 dt=#{dt.to_i} msg=#{msg}"
      ack
    end
    count += 1
  end
ensure
  puts "Published 150 msgs; Consumed #{count} msgs"
  B.close
end
