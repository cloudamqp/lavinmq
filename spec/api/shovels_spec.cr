require "../spec_helper.cr"
require "uri"

def create_shovel(server, name = "spec-shovel", config = NamedTuple.new, paused = false)
  config = NamedTuple.new(
    "src-uri": server.amqp_server.url,
    "dest-uri": server.amqp_server.url,
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
  describe "access control" do
    it "should refuse management users from listing all shovels" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/shovels", headers: hdrs)
        response.status_code.should eq 403
      end
    end

    it "should allow policymaker to list shovels in a vhost" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.get("/api/shovels/#{vhost_url_encoded}", headers: hdrs)
        response.status_code.should eq 200
      end
    end

    it "should refuse management and monitoring users from listing shovels" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management, LavinMQ::Tag::Monitoring])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.get("/api/shovels/#{vhost_url_encoded}", headers: hdrs)
        response.status_code.should eq 403
      end
    end

    it "should refuse management users from getting a shovel" do
      with_http_server do |http, s|
        create_shovel(s)
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.get("/api/shovels/#{vhost_url_encoded}/spec-shovel", headers: hdrs)
        response.status_code.should eq 403
      end
    end

    it "should refuse management users from pausing a shovel" do
      with_http_server do |http, s|
        create_shovel(s)
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.put("/api/shovels/#{vhost_url_encoded}/spec-shovel/pause", headers: hdrs)
        response.status_code.should eq 403
      end
    end

    it "should refuse management users from resuming a shovel" do
      with_http_server do |http, s|
        create_shovel(s, paused: true)
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        vhost_url_encoded = URI.encode_path_segment("/")
        response = http.put("/api/shovels/#{vhost_url_encoded}/spec-shovel/resume", headers: hdrs)
        response.status_code.should eq 403
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
