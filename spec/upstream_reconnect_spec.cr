require "./spec_helper"
require "../src/lavinmq/federation/upstream"
require "time_control"

# Busy-wait without sleeping: inside TimeControl.control all non-zero sleeps
# are virtualized, so wait_for (which sleeps) would deadlock the controlling
# fiber. Bounded by iterations since the clock stands still too.
private def spin_until(max_yields = 10_000_000, file = __FILE__, line = __LINE__, &)
  max_yields.times do
    return if yield
    Fiber.yield
  end
  fail "spin_until expired", file: file, line: line
end

describe LavinMQ::Federation::Upstream do
  it "should resync upstream exchange bindings on reconnect" do
    with_amqp_server do |s|
      upstream_vhost = s.vhosts.create("upstream")
      downstream_vhost = s.vhosts.create("downstream")
      upstream = LavinMQ::Federation::Upstream.new(
        downstream_vhost, "ef reconnect resync",
        "#{s.amqp_url}/upstream", "upstream_ex", nil,
        LavinMQ::Federation::AckMode::OnConfirm, expires: nil, max_hops: 1_i64,
        msg_ttl: nil, prefetch: 1000_u16, reconnect_delay: 1.second)
      downstream_vhost.upstreams.not_nil!.add(upstream)

      with_channel(s, vhost: "downstream") do |downstream_ch|
        downstream_ch.exchange("downstream_ex", "topic")
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "keep")
        downstream_q.bind("downstream_ex", "remove")

        definitions = {"federation-upstream" => JSON::Any.new(upstream.name)} of String => JSON::Any
        downstream_vhost.add_policy("FE", "downstream_ex", "exchanges", definitions, 12_i8)

        link = wait_for { upstream.links.first? }
        wait_for { link.state.running? }
        upstream_ex = wait_for { upstream_vhost.exchange?("upstream_ex") }
        upstream_ex = upstream_ex.as(LavinMQ::AMQP::Exchange)
        wait_for { upstream_ex.bindings_details.size == 2 }

        # Only control the link's fiber; the broker's housekeeping fibers
        # (RoughTime, stats loop, ...) keep re-arming periodic sleeps, which
        # would freeze and leak as pending timers.
        TimeControl.control(only: /^Federation link /) do |controller|
          # Kill the link's upstream connection; the link parks in
          # wait_before_reconnect on a now-virtual reconnect_delay timeout.
          upstream_vhost.each_connection do |conn|
            conn.close if conn.client_name.starts_with?("Federation link")
          end
          spin_until { link.state.stopped? }
          # Mutate downstream bindings while the link is disconnected
          downstream_q.unbind("downstream_ex", "remove")
          downstream_q.bind("downstream_ex", "added")
          # Margin over reconnect_delay: the timeout may register while the
          # advance is already in progress.
          controller.advance(2.seconds)
        end

        # The resync happens during link setup, before the state is Running
        wait_for { link.state.running? }
        upstream_ex.bindings_details.map(&.routing_key).sort!.should eq ["added", "keep"]
      end
    end
  end
end
