require "../spec_helper.cr"
require "uri"

def create_federation_link(server, upstream_name = "spec-upstream", exchange_name = "spec-ex")
  vhost = server.vhosts["/"]
  upstream = LavinMQ::Federation::Upstream.new(vhost, upstream_name, server.amqp_url, exchange_name)
  vhost.upstreams.add(upstream)

  vhost.declare_exchange(exchange_name, "topic", durable: false, auto_delete: false)
  exchange = vhost.exchanges[exchange_name]
  vhost.declare_queue("spec-q", durable: false, auto_delete: false)
  queue = vhost.queues["spec-q"]
  exchange.bind(queue, "#")

  link = upstream.link(exchange)
  wait_for { link.state.running? }
  {upstream, link}
end

describe LavinMQ::HTTP::MainController do
  describe "PUT /api/federation-links/:vhost/:upstream/:name/pause" do
    it "should return 404 for non-existing link" do
      with_http_server do |http, _s|
        vhost_url = URI.encode_path_segment("/")
        response = http.put("/api/federation-links/#{vhost_url}/non-existing/non-existing/pause")
        response.status_code.should eq 404
      end
    end

    it "should pause link and return 204" do
      with_http_server do |http, s|
        upstream, link = create_federation_link(s)
        vhost_url = URI.encode_path_segment("/")
        upstream_url = URI.encode_path_segment(upstream.name)
        link_url = URI.encode_path_segment(link.name)

        response = http.put("/api/federation-links/#{vhost_url}/#{upstream_url}/#{link_url}/pause")
        response.status_code.should eq 204

        link.state.paused?.should be_true
      ensure
        upstream.try &.close
      end
    end

    it "should return 422 if link is already paused" do
      with_http_server do |http, s|
        upstream, link = create_federation_link(s)
        link.pause
        wait_for { link.state.paused? }

        vhost_url = URI.encode_path_segment("/")
        upstream_url = URI.encode_path_segment(upstream.name)
        link_url = URI.encode_path_segment(link.name)

        response = http.put("/api/federation-links/#{vhost_url}/#{upstream_url}/#{link_url}/pause")
        response.status_code.should eq 422
      ensure
        upstream.try &.close
      end
    end
  end

  describe "PUT /api/federation-links/:vhost/:upstream/:name/resume" do
    it "should return 404 for non-existing link" do
      with_http_server do |http, _s|
        vhost_url = URI.encode_path_segment("/")
        response = http.put("/api/federation-links/#{vhost_url}/non-existing/non-existing/resume")
        response.status_code.should eq 404
      end
    end

    it "should resume a paused link and return 204" do
      with_http_server do |http, s|
        upstream, link = create_federation_link(s)
        link.pause
        wait_for { link.state.paused? }

        vhost_url = URI.encode_path_segment("/")
        upstream_url = URI.encode_path_segment(upstream.name)
        link_url = URI.encode_path_segment(link.name)

        response = http.put("/api/federation-links/#{vhost_url}/#{upstream_url}/#{link_url}/resume")
        response.status_code.should eq 204

        wait_for { link.state.running? }
      ensure
        upstream.try &.close
      end
    end

    it "should return 422 if link is already running" do
      with_http_server do |http, s|
        upstream, link = create_federation_link(s)

        vhost_url = URI.encode_path_segment("/")
        upstream_url = URI.encode_path_segment(upstream.name)
        link_url = URI.encode_path_segment(link.name)

        response = http.put("/api/federation-links/#{vhost_url}/#{upstream_url}/#{link_url}/resume")
        response.status_code.should eq 422
      ensure
        upstream.try &.close
      end
    end
  end
end
