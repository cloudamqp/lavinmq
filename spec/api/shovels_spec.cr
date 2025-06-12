require "../spec_helper.cr"
require "uri"

describe LavinMQ::HTTP::ShovelsController do
  describe "PUT api/shovels/:vhost/:pause" do
    with_http_server do |http, s|
      vhost_url_encoded = URI.encode_path_segment("/")
      body = %({
        "value": {
          "src-uri": "#{s.amqp_url}",
          "dest-uri": "#{s.amqp_url}",
          "dest-queue":"q1",
          "src-queue":"q2",
          "src-prefetch-count":1000,
          "src-delete-after":"never",
          "reconnect-delay":5,
          "ack-mode":"on-confirm"
        }
      })
      response = http.put("/api/parameters/shovel/#{vhost_url_encoded}/name", body: body)
      response.status_code.should eq 201
      wait_for {
        current_state = JSON.parse(http.get("/api/shovels/#{vhost_url_encoded}/name").body)["state"]
        current_state === "Running"
      }
      response = http.put("/api/shovels/#{vhost_url_encoded}/name/pause")
      response.status_code.should eq 204

      wait_for {
        current_state = JSON.parse(http.get("/api/shovels/#{vhost_url_encoded}/name").body)["state"]
        current_state === "Paused"
      }
    end
  end

  describe "PUT api/shovels/:vhost/:name/resume" do
    with_http_server do |http, s|
      vhost_url_encoded = URI.encode_path_segment("/")
      body = %({
          "value": {
            "src-uri": "#{s.amqp_url}",
            "dest-uri": "#{s.amqp_url}",
            "dest-queue":"q1",
            "src-queue":"q2",
            "src-prefetch-count":1000,
            "src-delete-after":"never",
            "reconnect-delay":5,
            "ack-mode":"on-confirm"
          }
        })
      response = http.put("/api/parameters/shovel/#{vhost_url_encoded}/name", body: body)
      response.status_code.should eq 201
      wait_for {
        current_state = JSON.parse(http.get("/api/shovels/#{vhost_url_encoded}/name").body)["state"]
        current_state === "Running"
      }
      response = http.put("/api/shovels/%2F/name/pause")
      response.status_code.should eq 204

      wait_for {
        current_state = JSON.parse(http.get("/api/shovels/#{vhost_url_encoded}/name").body)["state"]
        current_state === "Paused"
      }

      status_code = http.put("/api/shovels/#{vhost_url_encoded}/name/resume").status_code
      status_code.should eq 204

      wait_for {
        response = http.get("/api/shovels/#{vhost_url_encoded}/name")
        response.status_code.should eq 200
        current_state = JSON.parse(response.body)["state"]?
        current_state === "Running"
      }
    end
  end
end
