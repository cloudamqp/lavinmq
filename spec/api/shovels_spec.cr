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
  describe "PUT /api/shovels/:vhost/:name" do
    it "should create a new shovel and return 201" do
      with_http_server do |http, s|
        vhost_url_encoded = URI.encode_path_segment("/")
        config = {
          "src-uri": s.amqp_url,
          "dest-uri": s.amqp_url,
          "dest-queue": "q1",
          "src-queue": "q2",
          "src-prefetch-count": 1000,
          "src-delete-after": "never",
          "reconnect-delay": 5,
          "ack-mode": "on-confirm",
        }
        
        response = http.put("/api/shovels/#{vhost_url_encoded}/test-shovel", 
                           headers: HTTP::Headers{"Content-Type" => "application/json"},
                           body: config.to_json)
        response.status_code.should eq 201
        
        # Verify shovel was created
        shovel = s.vhosts["/"].shovels["test-shovel"]?
        shovel.should_not be_nil
      end
    end

    it "should update existing shovel and return 204" do
      with_http_server do |http, s|
        shovel = create_shovel(s, "existing-shovel")
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)
        
        config = {
          "src-uri": s.amqp_url,
          "dest-uri": s.amqp_url,
          "dest-queue": "q1",
          "src-queue": "q3", # Different queue
          "src-prefetch-count": 500, # Different prefetch
          "src-delete-after": "never",
          "reconnect-delay": 5,
          "ack-mode": "on-confirm",
        }
        
        response = http.put("/api/shovels/#{vhost_url_encoded}/existing-shovel",
                           headers: HTTP::Headers{"Content-Type" => "application/json"},
                           body: config.to_json)
        response.status_code.should eq 204
      end
    end

    it "should return 400 for missing required fields" do
      with_http_server do |http, _s|
        vhost_url_encoded = URI.encode_path_segment("/")
        config = {"src-uri": "amqp://localhost"} # Missing dest-uri
        
        response = http.put("/api/shovels/#{vhost_url_encoded}/bad-shovel",
                           headers: HTTP::Headers{"Content-Type" => "application/json"},
                           body: config.to_json)
        response.status_code.should eq 400
      end
    end
  end

  describe "DELETE /api/shovels/:vhost/:name" do
    it "should delete existing shovel and return 204" do
      with_http_server do |http, s|
        shovel = create_shovel(s, "delete-me")
        vhost_url_encoded = URI.encode_path_segment(shovel.vhost.name)

        response = http.delete("/api/shovels/#{vhost_url_encoded}/delete-me")
        response.status_code.should eq 204

        # Verify shovel was deleted
        s.vhosts["/"].shovels["delete-me"]?.should be_nil
      end
    end

    it "should return 404 for non-existing shovel" do
      with_http_server do |http, _s|
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.delete("/api/shovels/#{vhost_url_encoded}/non-existing")
        response.status_code.should eq 404
      end
    end
  end

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
