require "../spec_helper"

describe LavinMQ::HTTP::SimpleHttpController do
  describe "POST /api/simple/:vhost/:name" do
    it "auto-declares the queue and publishes a raw body" do
      with_http_server do |http, s|
        response = http.post("/api/simple/%2f/simple1", body: "hello world")
        response.status_code.should eq 201
        q = s.vhosts["/"].queues["simple1"].as(LavinMQ::AMQP::Queue)
        q.durable?.should be_true
        q.message_count.should eq 1
      end
    end

    it "reuses an existing queue instead of re-declaring" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("simple2", true, false)
        2.times { http.post("/api/simple/%2f/simple2", body: "x").status_code.should eq 201 }
        s.vhosts["/"].queues["simple2"].as(LavinMQ::AMQP::Queue).message_count.should eq 2
      end
    end

    it "rejects reserved-prefix queue names" do
      with_http_server do |http, _|
        response = http.post("/api/simple/%2f/amq.blocked", body: "x")
        response.status_code.should eq 400
      end
    end

    it "rejects payloads larger than max_message_size and accounts read bytes" do
      with_http_server do |http, s|
        original = LavinMQ::Config.instance.max_message_size
        LavinMQ::Config.instance.max_message_size = 16
        before = s.vhosts["/"].recv_oct_count
        begin
          response = http.post("/api/simple/%2f/toobig", body: "x" * 32)
          response.status_code.should eq 413
          s.update_stats_rates
          (s.vhosts["/"].recv_oct_count - before).should eq 17 # max_size + 1
        ensure
          LavinMQ::Config.instance.max_message_size = original
        end
      end
    end

    it "rejects publishing to a stream queue" do
      with_http_server do |http, s|
        args = LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})
        s.vhosts["/"].declare_queue("streamq", true, false, args)
        response = http.post("/api/simple/%2f/streamq", body: "x")
        response.status_code.should eq 409
      end
    end

    it "requires authentication" do
      with_http_server do |http, _|
        response = HTTP::Client.post(http.test_uri("/api/simple/%2f/noauth"), body: "x")
        response.status_code.should eq 401
      end
    end

    it "returns 403 when the user lacks write permission" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::Management])
        s.users.add_permission("limited", "/", /.*/, /.*/, /^$/) # config, read, write=(empty)
        headers = HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        response = http.post("/api/simple/%2f/writeblocked", body: "x", headers: headers)
        response.status_code.should eq 403
      end
    end

    it "returns 403 when the user lacks configure permission on a new queue" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::Management])
        s.users.add_permission("limited", "/", /^$/, /.*/, /.*/) # config=(empty), read, write
        headers = HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        response = http.post("/api/simple/%2f/configblocked", body: "x", headers: headers)
        response.status_code.should eq 403
      end
    end

    it "allows publishing when the queue already exists even without configure permission" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("preexisting", true, false)
        s.users.create("limited", "pw", [LavinMQ::Tag::Management])
        s.users.add_permission("limited", "/", /^$/, /.*/, /.*/)
        headers = HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        response = http.post("/api/simple/%2f/preexisting", body: "x", headers: headers)
        response.status_code.should eq 201
      end
    end

    it "updates vhost recv_oct and publish counters" do
      with_http_server do |http, s|
        vhost = s.vhosts["/"]
        before_recv = vhost.recv_oct_count
        before_publish = vhost.publish_count
        http.post("/api/simple/%2f/metricsq", body: "payload12")
        s.update_stats_rates
        (vhost.recv_oct_count - before_recv).should eq 9
        (vhost.publish_count - before_publish).should eq 1
      end
    end

    it "handles concurrent POSTs to a missing queue without errors" do
      with_http_server do |http, s|
        count = 20
        done = Channel(Int32).new(count)
        count.times do
          spawn do
            response = http.post("/api/simple/%2f/racey", body: "x")
            done.send(response.status_code)
          end
        end
        statuses = Array(Int32).new(count) { done.receive }
        statuses.all?(201).should be_true
        s.vhosts["/"].queues["racey"].as(LavinMQ::AMQP::Queue).message_count.should eq count
      end
    end
  end

  describe "POST /api/simple/:vhost/:name/get" do
    it "returns the oldest message body as raw bytes" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("getq", true, false)
        http.post("/api/simple/%2f/getq", body: "one")
        http.post("/api/simple/%2f/getq", body: "two")
        r1 = http.post("/api/simple/%2f/getq/get")
        r1.status_code.should eq 200
        r1.body.should eq "one"
        r1.headers["Content-Type"].should eq "application/octet-stream"
        r2 = http.post("/api/simple/%2f/getq/get")
        r2.body.should eq "two"
      end
    end

    it "returns 204 when the queue is empty" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("emptyq", true, false)
        response = http.post("/api/simple/%2f/emptyq/get")
        response.status_code.should eq 204
      end
    end

    it "returns 404 when the queue does not exist" do
      with_http_server do |http, _|
        response = http.post("/api/simple/%2f/missing/get")
        response.status_code.should eq 404
      end
    end

    it "rejects GET from a stream queue" do
      with_http_server do |http, s|
        args = LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})
        s.vhosts["/"].declare_queue("getstream", true, false, args)
        response = http.post("/api/simple/%2f/getstream/get")
        response.status_code.should eq 409
      end
    end

    it "returns 403 when the user lacks read permission" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("readblocked", true, false)
        s.users.create("limited", "pw", [LavinMQ::Tag::Management])
        s.users.add_permission("limited", "/", /.*/, /^$/, /.*/) # config, read=(empty), write
        headers = HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        response = http.post("/api/simple/%2f/readblocked/get", headers: headers)
        response.status_code.should eq 403
      end
    end

    it "round-trips binary payloads unchanged" do
      with_http_server do |http, _|
        payload = Bytes.new(256, &.to_u8)
        http.post("/api/simple/%2f/binq", body: String.new(payload)).status_code.should eq 201
        response = http.post("/api/simple/%2f/binq/get")
        response.status_code.should eq 200
        response.body.to_slice.should eq payload
      end
    end

    it "updates vhost send_oct and get_no_ack counters" do
      with_http_server do |http, s|
        vhost = s.vhosts["/"]
        http.post("/api/simple/%2f/getmetrics", body: "abc123")
        before_send = vhost.send_oct_count
        before_get = vhost.get_no_ack_count
        response = http.post("/api/simple/%2f/getmetrics/get")
        response.status_code.should eq 200
        s.update_stats_rates
        (vhost.send_oct_count - before_send).should eq 6
        (vhost.get_no_ack_count - before_get).should eq 1
      end
    end
  end
end
