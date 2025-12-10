require "../spec_helper.cr"
require "uri"
require "openssl/hmac"
require "base64"

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

  describe "HTTP shovel with signature secret" do
    it "should accept dest-signature-secret parameter and send Standard Webhooks headers" do
      with_http_server do |_http, s|
        # Setup HTTP server to receive webhook
        h = Hash(String, String).new
        received_body = ""
        server = HTTP::Server.new do |context|
          context.request.headers.each do |k, v|
            h[k] = v.first
          end
          received_body = context.request.body.try(&.gets_to_end) || ""
          context.response.content_type = "text/plain"
          context.response.print "ok"
        end
        addr = server.bind_unused_port
        spawn server.listen

        # Create HTTP shovel with signature secret via store
        with_channel(s) do |ch|
          ch.queue("http_sig_src")
        end

        secret = "test-secret-123"
        config = {
          "src-uri":               s.amqp_url,
          "src-queue":             "http_sig_src",
          "dest-uri":              "http://#{addr}/webhook",
          "dest-signature-secret": secret,
          "src-delete-after":      "queue-length",
        }
        shovel = s.vhosts["/"].shovels.create("http-sig-shovel", JSON.parse(config.to_json))

        # Publish a message
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          x.publish_confirm "test payload", "http_sig_src"
        end

        # Wait for shovel to process
        wait_for { shovel.state.terminated? }
        sleep 10.milliseconds

        # Verify Standard Webhooks headers were sent
        h["webhook-id"]?.should_not be_nil
        h["webhook-timestamp"]?.should_not be_nil
        h["webhook-signature"]?.should_not be_nil

        # Verify signature format and correctness
        webhook_id = h["webhook-id"]
        timestamp = h["webhook-timestamp"]
        signature_header = h["webhook-signature"]

        webhook_id.should start_with "msg_"
        signature_header.should start_with "v1,"

        signed_content = "#{webhook_id}.#{timestamp}.#{received_body}"
        digest = OpenSSL::HMAC.digest(OpenSSL::Algorithm::SHA256, secret, signed_content)
        expected_signature = Base64.strict_encode(digest)
        signature_header.should eq "v1,#{expected_signature}"
      end
    end
  end
end
