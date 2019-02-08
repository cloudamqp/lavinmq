require 'bunny'
B = Bunny.new('amqp://localhost')
B.start

def publish_100_msgs(qname)
  pub_ch = B.create_channel
  100.times { |i| pub_ch.direct('').publish("#{i}", routing_key: qname) }
end
def get(qname, ch)
  delivery, _headers, body = ch.basic_get(qname, manual_ack: true)
  yield body
  ch.acknowledge(delivery.delivery_tag, false)
end
count = 0
ch = B.create_channel(nil, 1)
ch.queue('test_get1', durable: false, auto_delete: true)
publish_100_msgs('test_get1')
ch.prefetch 100
begin
  150.times do |i|
    get('test_get1', ch) do |msg|
      puts "queue=test_get1 msg=#{msg}"
      ch.basic_recover(true) if count == 50
    end
    count += 1
  end
ensure
  puts "Published 150 msgs; Consumed #{count} msgs"
end

