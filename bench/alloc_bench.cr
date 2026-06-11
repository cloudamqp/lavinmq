# Allocation benchmark for the message hot path: publish -> store -> deliver.
#
# Measures GC-allocated bytes per operation for the server-side code paths,
# in-process (no sockets), so numbers are attributable to LavinMQ itself.
#
# Usage:
#   crystal run --release bench/alloc_bench.cr
require "../src/lavinmq/config"
require "../src/lavinmq/server"
require "../src/lavinmq/http/http_server"
require "file_utils"

::Log.setup(:fatal)

data_dir = File.tempname("lavinmq-alloc-bench")
Dir.mkdir_p(data_dir)
config = LavinMQ::Config.instance
config.data_dir = data_dir
config.segment_size = 64 * 1024 * 1024

server = LavinMQ::Server.new(config)
vhost = server.vhosts["/"]

vhost.declare_queue("bench_q", true, false)
queue = vhost.queue?("bench_q").not_nil!

vhost.declare_exchange("bench_direct", "direct", true, false)
vhost.declare_exchange("bench_topic", "topic", true, false)
vhost.declare_exchange("bench_fanout", "fanout", true, false)
vhost.declare_exchange("bench_headers", "headers", true, false)
vhost.bind_queue("bench_q", "bench_direct", "bound")
vhost.bind_queue("bench_q", "bench_topic", "a.b.c")
vhost.bind_queue("bench_q", "bench_fanout", "")
vhost.bind_queue("bench_q", "bench_headers", "", LavinMQ::AMQP::Table.new({"x-match" => "all", "app" => "bench"}))

BODY = Bytes.new(64, 120u8)

def make_msg(exchange : String, rk : String, props = AMQ::Protocol::Properties.new) : LavinMQ::Message
  LavinMQ::Message.new(RoughTime.unix_ms, exchange, rk, props, BODY.size.to_u64, IO::Memory.new(BODY))
end

visited = Set(LavinMQ::Exchange).new
found_queues = Set(LavinMQ::Queue).new

def measure(name : String, n : Int32, &)
  GC.collect
  before = GC.stats.total_bytes
  yield
  after = GC.stats.total_bytes
  printf "%-44s %9.1f bytes/op   (%d ops)\n", name, (after &- before).to_f / n, n
end

def drain(queue, n)
  null = File.open("/dev/null", "w")
  n.times do
    queue.basic_get(true) { |env| env }
  end
  null.close
end

N = 50_000

# warmup all paths once
warm = make_msg("", "bench_q")
1000.times do
  vhost.publish(warm, false, visited, found_queues)
  warm.body_io.rewind
end
drain(queue, 1000)

puts "== publish path (vhost.publish -> exchange -> queue -> message store) =="

msg = make_msg("", "bench_q")
measure("publish: default exchange -> queue", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

msg = make_msg("bench_direct", "bound")
measure("publish: direct exchange, bound rk", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

msg = make_msg("bench_direct", "unbound")
measure("publish: direct exchange, unbound rk (same)", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end

# unique unbound routing keys: exposes per-key allocations. The String itself
# accounts for ~32 bytes/op of the result.
rks = Array(String).new(N) { |i| "unbound.#{i}" }
msgs = rks.map { |rk| make_msg("bench_direct", rk) }
measure("publish: direct exchange, unbound rk (uniq)", N) do
  msgs.each do |m|
    vhost.publish(m, false, visited, found_queues)
  end
end

msg = make_msg("bench_topic", "a.b.c")
measure("publish: topic exchange, matching rk", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

msg = make_msg("bench_fanout", "")
measure("publish: fanout exchange", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

hdr_props = AMQ::Protocol::Properties.new(headers: LavinMQ::AMQP::Table.new({"app" => "bench"}))
msg = make_msg("bench_headers", "", hdr_props)
measure("publish: headers exchange, matching msg", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

big_hdr_props = AMQ::Protocol::Properties.new(headers: LavinMQ::AMQP::Table.new({
  "app" => "bench", "tenant" => "t-1234", "trace-id" => "0123456789abcdef", "retries" => 3,
}))
msg = make_msg("", "bench_q", big_hdr_props)
measure("publish: default exchange, msg w/ 4 headers", N) do
  N.times do
    vhost.publish(msg, false, visited, found_queues)
    msg.body_io.rewind
  end
end
drain(queue, N)

puts
puts "== deliver path (queue.basic_get no_ack -> frame encode to /dev/null) =="

null = File.open("/dev/null", "w")

msg = make_msg("", "bench_q")
N.times do
  vhost.publish(msg, false, visited, found_queues)
  msg.body_io.rewind
end
measure("deliver: plain msg", N) do
  N.times do
    queue.basic_get(true) do |env|
      m = env.message
      deliver = AMQ::Protocol::Frame::Basic::Deliver.new(1_u16, "ctag", 1_u64, false, m.exchange_name, m.routing_key)
      null.write_bytes deliver, IO::ByteFormat::NetworkEndian
      header = AMQ::Protocol::Frame::Header.new(1_u16, 60_u16, 0_u16, m.bodysize, m.properties)
      null.write_bytes header, IO::ByteFormat::NetworkEndian
      body = AMQ::Protocol::Frame::BytesBody.new(1_u16, m.bodysize.to_u32, m.body)
      null.write_bytes body, IO::ByteFormat::NetworkEndian
    end
  end
end

msg = make_msg("", "bench_q", big_hdr_props)
N.times do
  vhost.publish(msg, false, visited, found_queues)
  msg.body_io.rewind
end
measure("deliver: msg w/ 4 headers", N) do
  N.times do
    queue.basic_get(true) do |env|
      m = env.message
      deliver = AMQ::Protocol::Frame::Basic::Deliver.new(1_u16, "ctag", 1_u64, false, m.exchange_name, m.routing_key)
      null.write_bytes deliver, IO::ByteFormat::NetworkEndian
      header = AMQ::Protocol::Frame::Header.new(1_u16, 60_u16, 0_u16, m.bodysize, m.properties)
      null.write_bytes header, IO::ByteFormat::NetworkEndian
      body = AMQ::Protocol::Frame::BytesBody.new(1_u16, m.bodysize.to_u32, m.body)
      null.write_bytes body, IO::ByteFormat::NetworkEndian
    end
  end
end

puts
puts "== components =="

# serialize a message with headers to a buffer, then measure read-side pieces
buf_io = IO::Memory.new
msg = make_msg("", "bench_q", big_hdr_props)
buf_io.write_bytes msg, IO::ByteFormat::SystemEndian
buf = buf_io.to_slice

measure("component: BytesMessage.from_bytes (4 hdrs)", N) do
  N.times do
    LavinMQ::BytesMessage.from_bytes(buf)
  end
end

bmsg = LavinMQ::BytesMessage.from_bytes(buf)
measure("component: SegmentPosition.make (4 hdrs)", N) do
  N.times do
    LavinMQ::SegmentPosition.make(1u32, 4u32, bmsg)
  end
end

measure("component: frame encode (4 hdrs)", N) do
  N.times do
    deliver = AMQ::Protocol::Frame::Basic::Deliver.new(1_u16, "ctag", 1_u64, false, bmsg.exchange_name, bmsg.routing_key)
    null.write_bytes deliver, IO::ByteFormat::NetworkEndian
    header = AMQ::Protocol::Frame::Header.new(1_u16, 60_u16, 0_u16, bmsg.bodysize, bmsg.properties)
    null.write_bytes header, IO::ByteFormat::NetworkEndian
    body = AMQ::Protocol::Frame::BytesBody.new(1_u16, bmsg.bodysize.to_u32, bmsg.body)
    null.write_bytes body, IO::ByteFormat::NetworkEndian
  end
end

plain_buf_io = IO::Memory.new
msg = make_msg("", "bench_q")
plain_buf_io.write_bytes msg, IO::ByteFormat::SystemEndian
plain_buf = plain_buf_io.to_slice

measure("component: BytesMessage.from_bytes (plain)", N) do
  N.times do
    LavinMQ::BytesMessage.from_bytes(plain_buf)
  end
end

null.close
server.close
FileUtils.rm_rf(data_dir)
