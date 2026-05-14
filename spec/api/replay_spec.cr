require "../spec_helper"

private def publish_replay(s, queue : String, body : String, source : String = "src")
  with_channel(s) do |ch|
    if s.vhosts["/"].queue?(queue).nil?
      ch.queue(queue, durable: true,
        args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
    end
    props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
      "x-source-queue"       => source,
      "x-source-exchange"    => "",
      "x-source-routing-key" => source,
    }))
    ch.basic_publish_confirm(body, "", queue, props: props)
  end
end

private def replay_id_of(s, queue : String) : String
  q = s.vhosts["/"].queue?(queue).as(LavinMQ::AMQP::ReplayQueue)
  id = nil
  q.each_envelope do |env|
    id = env.message.properties.headers.not_nil![LavinMQ::Replay::HEADER_REPLAY_ID].to_s
    break
  end
  id.not_nil!
end

describe "replay HTTP API" do
  describe "GET /api/replay/:vhost" do
    it "lists every replay queue in the vhost with message counts" do
      with_http_server do |http, s|
        publish_replay(s, "rep-list-a", "a")
        publish_replay(s, "rep-list-b", "b1")
        publish_replay(s, "rep-list-b", "b2")
        sleep 20.milliseconds
        response = http.get("/api/replay/%2f")
        response.status_code.should eq 200
        list = JSON.parse(response.body).as_a
        names = list.map(&.["name"].as_s)
        names.includes?("rep-list-a").should be_true
        names.includes?("rep-list-b").should be_true
        b = list.find! { |q| q["name"].as_s == "rep-list-b" }
        b["messages"].as_i.should eq 2
        b["durable"].as_bool.should be_true
      end
    end

    it "ignores regular queues" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("plain-q", true, false)
        publish_replay(s, "only-replay", "x")
        sleep 20.milliseconds
        response = http.get("/api/replay/%2f")
        names = JSON.parse(response.body).as_a.map(&.["name"].as_s)
        names.includes?("plain-q").should be_false
        names.includes?("only-replay").should be_true
      end
    end
  end

  describe "GET /api/replay/:vhost/:name" do
    it "returns one item per message with origin metadata" do
      with_http_server do |http, s|
        publish_replay(s, "rep-items", %({"k":"v"}))
        sleep 20.milliseconds
        response = http.get("/api/replay/%2f/rep-items")
        response.status_code.should eq 200
        items = JSON.parse(response.body).as_a
        items.size.should eq 1
        items[0]["source"].as_s.should eq "src"
        items[0]["payload_bytes"].as_i.should eq %({"k":"v"}).bytesize
        items[0]["id"].as_s.size.should be > 0
      end
    end

    it "returns 404 when the queue isn't a replay queue" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("regular", true, false)
        response = http.get("/api/replay/%2f/regular")
        response.status_code.should eq 404
      end
    end

    it "returns 404 when the queue doesn't exist" do
      with_http_server do |http, _|
        response = http.get("/api/replay/%2f/no-such")
        response.status_code.should eq 404
      end
    end
  end

  describe "GET /api/replay/:vhost/:name/:id" do
    it "returns full payload + properties for an existing id" do
      with_http_server do |http, s|
        publish_replay(s, "rep-single", %({"k":"v"}))
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-single")
        response = http.get("/api/replay/%2f/rep-single/#{id}")
        response.status_code.should eq 200
        data = JSON.parse(response.body)
        data["payload"].as_s.should eq %({"k":"v"})
        data["payload_encoding"].as_s.should eq "string"
        data["source"].as_s.should eq "src"
        data["properties"]["headers"]["x-source-queue"].as_s.should eq "src"
      end
    end

    it "returns 404 for an unknown id" do
      with_http_server do |http, s|
        publish_replay(s, "rep-miss", "x")
        sleep 20.milliseconds
        response = http.get("/api/replay/%2f/rep-miss/no-such-id")
        response.status_code.should eq 404
      end
    end
  end

  describe "POST /api/replay/:vhost/:name/:id/release" do
    it "republishes to the source via amq.default and removes from replay" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("rep-release-src", true, false)
        with_channel(s) do |ch|
          ch.queue("rep-release", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
            "x-source-queue"       => "rep-release-src",
            "x-source-exchange"    => "",
            "x-source-routing-key" => "rep-release-src",
            "user-key"             => "user-value",
          }))
          ch.basic_publish_confirm("payload", "", "rep-release", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-release")
        response = http.post("/api/replay/%2f/rep-release/#{id}/release", body: "")
        response.status_code.should eq 204
        sleep 20.milliseconds
        s.vhosts["/"].queue?("rep-release").not_nil!.message_count.should eq 0
        src = s.vhosts["/"].queue?("rep-release-src").not_nil!
        src.message_count.should eq 1
      end
    end

    it "keeps x-source-queue by default so the source's filter will skip it" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("rep-marker-src", true, false)
        with_channel(s) do |ch|
          ch.queue("rep-marker", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
            "x-source-queue"       => "rep-marker-src",
            "x-source-exchange"    => "",
            "x-source-routing-key" => "rep-marker-src",
          }))
          ch.basic_publish_confirm("p", "", "rep-marker", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-marker")
        http.post("/api/replay/%2f/rep-marker/#{id}/release", body: "").status_code.should eq 204
        sleep 20.milliseconds
        with_channel(s) do |ch|
          msg = ch.basic_get("rep-marker-src", no_ack: true).not_nil!
          h = msg.properties.headers.not_nil!
          h["x-source-queue"].should eq "rep-marker-src"
          h.has_key?("x-replay-id").should be_false
          h.has_key?("x-source-timestamp").should be_false
        end
      end
    end

    it "with ?reset_replay=true strips x-source-* so the filter will re-evaluate" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("rep-reset-src", true, false)
        with_channel(s) do |ch|
          ch.queue("rep-reset", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
            "x-source-queue"       => "rep-reset-src",
            "x-source-exchange"    => "",
            "x-source-routing-key" => "rep-reset-src",
            "user-key"             => "v",
          }))
          ch.basic_publish_confirm("p", "", "rep-reset", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-reset")
        http.post("/api/replay/%2f/rep-reset/#{id}/release?reset_replay=true", body: "").status_code.should eq 204
        sleep 20.milliseconds
        with_channel(s) do |ch|
          msg = ch.basic_get("rep-reset-src", no_ack: true).not_nil!
          h = msg.properties.headers.not_nil!
          h.has_key?("x-source-queue").should be_false
          h["user-key"].should eq "v"
        end
      end
    end

    it "returns 404 for an unknown id" do
      with_http_server do |http, s|
        publish_replay(s, "rep-release-miss", "x")
        sleep 20.milliseconds
        response = http.post("/api/replay/%2f/rep-release-miss/no-such/release", body: "")
        response.status_code.should eq 404
      end
    end
  end

  describe "counters" do
    it "release bumps replay_released_count" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("cnt-source", true, false)
        publish_replay(s, "cnt-rel", "p", source: "cnt-source")
        sleep 20.milliseconds
        id = replay_id_of(s, "cnt-rel")
        http.post("/api/replay/%2f/cnt-rel/#{id}/release", body: "").status_code.should eq 204
        q = s.vhosts["/"].queue?("cnt-rel").as(LavinMQ::AMQP::ReplayQueue)
        q.replay_released_count.should eq 1
        q.replay_edited_count.should eq 0
      end
    end

    it "patch bumps replay_edited_count" do
      with_http_server do |http, s|
        props = AMQP::Client::Properties.new(
          content_type: "application/json",
          headers: AMQP::Client::Arguments.new({
            "x-source-queue"       => "src",
            "x-source-exchange"    => "",
            "x-source-routing-key" => "src",
          }),
        )
        with_channel(s) do |ch|
          ch.queue("cnt-edit", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          ch.basic_publish_confirm(%({"a":1}), "", "cnt-edit", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "cnt-edit")
        http.patch("/api/replay/%2f/cnt-edit/#{id}", body: %({"body":"{\\"a\\":2}"})).status_code.should eq 204
        q = s.vhosts["/"].queue?("cnt-edit").as(LavinMQ::AMQP::ReplayQueue)
        q.replay_edited_count.should eq 1
        q.replay_released_count.should eq 0
      end
    end
  end

  describe "PATCH /api/replay/:vhost/:name/:id" do
    private_text_props = AMQP::Client::Properties.new(
      content_type: "application/json",
      headers: AMQP::Client::Arguments.new({
        "x-source-queue"       => "src",
        "x-source-exchange"    => "",
        "x-source-routing-key" => "src",
        "user-key"             => "old-value",
      }),
    )

    it "edits the body for editable content types" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("rep-patch", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          ch.basic_publish_confirm(%({"a":1}), "", "rep-patch", props: private_text_props)
        end
        sleep 20.milliseconds
        old_id = replay_id_of(s, "rep-patch")
        body = %({"body":"{\\"a\\":2}"})
        response = http.patch("/api/replay/%2f/rep-patch/#{old_id}", body: body)
        response.status_code.should eq 204
        new_id = replay_id_of(s, "rep-patch")
        new_id.should_not eq old_id
        get = http.get("/api/replay/%2f/rep-patch/#{new_id}")
        get.status_code.should eq 200
        data = JSON.parse(get.body)
        data["payload"].as_s.should eq %({"a":2})
        data["properties"]["headers"]["x-source-queue"].as_s.should eq "src"
      end
    end

    it "replaces user headers but preserves x-source-*" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("rep-headers", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          ch.basic_publish_confirm("p", "", "rep-headers", props: private_text_props)
        end
        sleep 20.milliseconds
        old_id = replay_id_of(s, "rep-headers")
        body = %({"headers":{"user-key":"new-value","added":"yes"}})
        http.patch("/api/replay/%2f/rep-headers/#{old_id}", body: body).status_code.should eq 204
        new_id = replay_id_of(s, "rep-headers")
        get = JSON.parse(http.get("/api/replay/%2f/rep-headers/#{new_id}").body)
        h = get["properties"]["headers"]
        h["x-source-queue"].as_s.should eq "src"
        h["user-key"].as_s.should eq "new-value"
        h["added"].as_s.should eq "yes"
      end
    end

    it "ignores attempts to set x-source-* via headers" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("rep-x-source", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          ch.basic_publish_confirm("p", "", "rep-x-source", props: private_text_props)
        end
        sleep 20.milliseconds
        old_id = replay_id_of(s, "rep-x-source")
        body = %({"headers":{"x-source-queue":"hacker"}})
        http.patch("/api/replay/%2f/rep-x-source/#{old_id}", body: body).status_code.should eq 204
        new_id = replay_id_of(s, "rep-x-source")
        get = JSON.parse(http.get("/api/replay/%2f/rep-x-source/#{new_id}").body)
        get["properties"]["headers"]["x-source-queue"].as_s.should eq "src"
      end
    end

    it "rejects body edit for non-editable content types without force" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("rep-binary", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          props = AMQP::Client::Properties.new(
            content_type: "application/octet-stream",
            headers: AMQP::Client::Arguments.new({
              "x-source-queue"       => "src",
              "x-source-exchange"    => "",
              "x-source-routing-key" => "src",
            }),
          )
          ch.basic_publish_confirm("\x01\x02", "", "rep-binary", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-binary")
        response = http.patch("/api/replay/%2f/rep-binary/#{id}", body: %({"body":"text"}))
        response.status_code.should eq 415
      end
    end

    it "force=true overrides the content-type gate" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("rep-force", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          props = AMQP::Client::Properties.new(
            content_type: "application/octet-stream",
            headers: AMQP::Client::Arguments.new({
              "x-source-queue"       => "src",
              "x-source-exchange"    => "",
              "x-source-routing-key" => "src",
            }),
          )
          ch.basic_publish_confirm("\x01\x02", "", "rep-force", props: props)
        end
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-force")
        response = http.patch("/api/replay/%2f/rep-force/#{id}?force=true", body: %({"body":"new"}))
        response.status_code.should eq 204
      end
    end

    it "returns 404 for an unknown id" do
      with_http_server do |http, s|
        publish_replay(s, "rep-patch-miss", "x")
        sleep 20.milliseconds
        response = http.patch("/api/replay/%2f/rep-patch-miss/no-such-id", body: %({"body":"y"}))
        response.status_code.should eq 404
      end
    end
  end

  describe "DELETE /api/replay/:vhost/:name/:id" do
    it "purges the single message" do
      with_http_server do |http, s|
        publish_replay(s, "rep-del", "x")
        sleep 20.milliseconds
        id = replay_id_of(s, "rep-del")
        response = http.delete("/api/replay/%2f/rep-del/#{id}")
        response.status_code.should eq 204
        s.vhosts["/"].queue?("rep-del").not_nil!.message_count.should eq 0
      end
    end

    it "returns 404 for an unknown id" do
      with_http_server do |http, s|
        publish_replay(s, "rep-del-miss", "x")
        sleep 20.milliseconds
        response = http.delete("/api/replay/%2f/rep-del-miss/no-such-id")
        response.status_code.should eq 404
      end
    end
  end
end
