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

  describe "GET api/parameters/shovel" do
    it "should mask dest-signature-secret in response" do
      with_http_server do |http, s|
        # Create a shovel parameter with a signature secret via API
        body = {
          value: {
            "src-uri":               s.amqp_url,
            "dest-uri":              "https://example.com/webhook",
            "src-queue":             "test-q",
            "dest-signature-secret": "my-secret-key",
          },
        }
        response = http.put("/api/parameters/shovel/%2F/secret-shovel", body: body.to_json)
        response.status_code.should eq 201

        # Fetch and verify secret is masked in response
        response = http.get("/api/parameters/shovel/%2F/secret-shovel")
        response.status_code.should eq 200
        json = JSON.parse(response.body)
        json["value"]["dest-signature-secret"].as_s.should eq "********"
      end
    end

    it "should clear dest-signature-secret when updating without providing it" do
      with_http_server do |http, s|
        # Create a shovel parameter with a signature secret
        body = {
          value: {
            "src-uri":               s.amqp_url,
            "dest-uri":              "https://example.com/webhook",
            "src-queue":             "test-q",
            "dest-signature-secret": "my-secret-key",
          },
        }
        response = http.put("/api/parameters/shovel/%2F/clear-secret-shovel", body: body.to_json)
        response.status_code.should eq 201

        # Update without providing the secret - should clear it
        body = {
          value: {
            "src-uri":   s.amqp_url,
            "dest-uri":  "https://example.com/webhook",
            "src-queue": "updated-q",
          },
        }
        response = http.put("/api/parameters/shovel/%2F/clear-secret-shovel", body: body.to_json)
        response.status_code.should eq 204

        # Verify update was applied and secret is no longer set
        response = http.get("/api/parameters/shovel/%2F/clear-secret-shovel")
        json = JSON.parse(response.body)
        json["value"]["src-queue"].as_s.should eq "updated-q"
        json["value"].as_h.has_key?("dest-signature-secret").should be_false
      end
    end
  end
end
