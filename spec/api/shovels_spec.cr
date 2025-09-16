require "../spec_helper.cr"
require "uri"

def create_shovel(server, name = "spec-shovel", config = NamedTuple.new, paused = false)
  config = NamedTuple.new(
    "src-uri": server.amqp_url,
    "dest-uri": server.amqp_url,
    "dest-queue": "q1",
    "src-queue": "q2",
    "src-prefetch-count": 1000,
    "src-delete-after": "never",
    "reconnect-delay": 5,
    "ack-mode": "on-confirm",
  ).merge(config)
  shovel = server.vhosts["/"].shovels.create(name, JSON.parse(config.to_json))
  wait_for { shovel.state.running? }
  if paused
    shovel.pause
    wait_for { shovel.state.paused? }
  end
  shovel
end

describe LavinMQ::HTTP::ShovelsController do
  describe "PUT api/shovels/:vhost/:name/pause" do
    it "should return 404 for non-existing shovel" do
      with_http_server do |http, _s|
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.put("/api/shovels/#{vhost_url_encoded}/non-existing/pause")
        response.status_code.should eq 404
      end
    end

    it "should pause shovel and return 204" do
      with_http_server do |http, s|
        shovel = create_shovel(s)
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)

        response = http.put("/api/shovels/#{vhost_url_encoded}/#{shovel.name}/pause")
        response.status_code.should eq 204

        shovel.state.paused?.should be_true
      end
    end

    it "should return 422 if shovel is already paused" do
      with_http_server do |http, s|
        shovel = create_shovel(s, paused: true)
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)

        response = http.put("/api/shovels/#{vhost_url_encoded}/#{shovel.name}/pause")
        response.status_code.should eq 422
      end
    end
  end

  describe "PUT api/shovels/:vhost/:name/resume" do
    it "should return 404 for non-existing shovel" do
      with_http_server do |http, _s|
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.put("/api/shovels/#{vhost_url_encoded}/non-existing/resume")
        response.status_code.should eq 404
      end
    end

    it "should resume a paused shovel and return 204" do
      with_http_server do |http, s|
        shovel = create_shovel(s, paused: true)
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)
        status_code = http.put("/api/shovels/#{vhost_url_encoded}/#{shovel.name}/resume").status_code
        status_code.should eq 204

        wait_for { shovel.state.running? }
      end
    end

    it "should return 422 if shovel is already running" do
      with_http_server do |http, s|
        shovel = create_shovel(s, paused: false)
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)
        status_code = http.put("/api/shovels/#{vhost_url_encoded}/#{shovel.name}/resume").status_code
        status_code.should eq 422
      end
    end
  end
end
