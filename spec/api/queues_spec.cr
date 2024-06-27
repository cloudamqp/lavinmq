require "../spec_helper"
require "compress/deflate"

describe LavinMQ::HTTP::QueuesController do
  describe "GET /api/queues" do
    it "should return all queues" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("q0", false, false)
        response = http.get("/api/queues")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "durable", "exclusive", "auto_delete", "arguments", "consumers", "vhost",
                "messages", "ready", "unacked", "policy", "exclusive_consumer_tag", "state",
                "effective_policy_definition"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end
  describe "GET /api/queues/vhost" do
    it "should return all queues for a vhost" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("q0", false, false)
        response = http.get("/api/queues/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
  describe "GET /api/queues/vhost/name" do
    it "should return queue" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("q0", false, false)
        response = http.get("/api/queues/%2f/q0")
        response.status_code.should eq 200
      end
    end

    it "should return 404 if queue does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/queues/%2f/404")
        response.status_code.should eq 404
      end
    end

    it "should return message stats" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("stats_q")
          q.publish "m1"
        end
        response = http.get("/api/queues/%2f/stats_q")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["message_stats"]["publish_details"]["rate"].nil?.should be_false
      end
    end

    it "should return no persistent message count" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("stats_q", auto_delete: false, durable: false, exclusive: false)
          q.publish "m1"
        end
        response = http.get("/api/queues/%2f/stats_q")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["messages"].should eq 1
        body["messages_persistent"].should eq 0
      end
    end

    it "should return persistent message count" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("stats_q", auto_delete: false, durable: true, exclusive: false)
          q.publish "m1"
        end
        response = http.get("/api/queues/%2f/stats_q")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["messages"].should eq 1
        body["messages_persistent"].should eq 1
      end
    end
  end

  describe "GET /api/queues/vhost/name/unacked" do
    describe "subscribe" do
      it "should return unacked messages" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            2.times { q.publish "m1" }
            q.subscribe(no_ack: false) { }

            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 2 }
            s.vhosts["/"].queues["unacked_q"].basic_get_unacked.size.should eq 0
            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 2
          end
        end
      end

      it "should not return if acked" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            2.times { q.publish "m1" }

            q.subscribe(no_ack: false) do |msg|
              msg.ack
            end
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 0 }

            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 0
          end
        end
      end

      it "should not return if rejected" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            2.times { q.publish "m1" }

            q.subscribe(no_ack: false) do |msg|
              msg.reject
            end
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 0 }

            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 0
          end
        end
      end
    end

    describe "basic get" do
      it "should return unacked messages" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            q.publish "m1"

            msg = q.get(no_ack: false)
            wait_for { s.vhosts["/"].queues["unacked_q"].basic_get_unacked.size == 1 }
            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 1
          end
        end
      end

      it "should not return if acked" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            q.publish "m1"
            ch.prefetch(1)
            msg = q.get(no_ack: false)
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 1 }

            msg.not_nil!.ack
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 0 }
            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 0
          end
        end
      end

      it "should not return if rejected" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            q.publish "m1"

            ch.prefetch(1)
            msg = q.get(no_ack: false)
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 1 }
            msg.not_nil!.reject

            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 0 }
            response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body["items"].size.should eq 0
          end
        end
      end

      it "should be removed from unacked when channel closes" do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            q = ch.queue("unacked_q")
            q.publish "m1"

            ch.prefetch(1)
            msg = q.get(no_ack: false)
            wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 1 }
          end
          wait_for { s.vhosts["/"].queues["unacked_q"].unacked_count == 0 }
          response = http.get("/api/queues/%2f/unacked_q/unacked?page=1&page_size=100")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["items"].size.should eq 0
        end
      end
    end
  end

  describe "GET /api/queues/vhost/name/bindings" do
    it "should return queue bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("q0", false, false)
        s.vhosts["/"].bind_queue("q0", "amq.direct", "foo")
        response = http.get("/api/queues/%2f/q0/bindings?page=1&page_size=100")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["items"].as_a.size.should eq 2
      end
    end
  end
  describe "PUT /api/queues/vhost/name" do
    it "should create queue" do
      with_http_server do |http, _|
        body = {
          "durable"     => false,
          "auto_delete" => true,
          "arguments"   => {
            "max-length" => 10,
          },
        }
        response = http.put("/api/queues/%2f/putqueue", body: body.to_json)
        response.status_code.should eq 201
        response = http.get("/api/queues/%2f/putqueue")
        response.status_code.should eq 200
        json = JSON.parse(response.body)
        json["name"].should eq "putqueue"
        body.each do |key, value|
          json[key].should eq value
        end
      end
    end

    it "should not require any body" do
      with_http_server do |http, _|
        response = http.put("/api/queues/%2f/okq")
        response.status_code.should eq 201
      end
    end

    it "should require durable to be the same when overwriting" do
      with_http_server do |http, _|
        body = %({
        "durable": true
      })
        response = http.put("/api/queues/%2f/q1d", body: body)
        response.status_code.should eq 201
        body = %({
        "durable": false
      })
        response = http.put("/api/queues/%2f/q1d", body: body)
        response.status_code.should eq 400
      end
    end

    it "should not be possible to declare amq. prefixed queues" do
      with_http_server do |http, _|
        response = http.put("/api/queues/%2f/amq.test", body: %({}))
        response.status_code.should eq 400
      end
    end

    it "should respond with 400 for PreconditionFailed errors" do
      with_http_server do |http, _|
        # supplying 'x-dead-letter-routing-key' is only valid if
        # 'x-dead-letter-exchange' is also in the request
        # so this request generates a Error::PreconditionFailed
        body = %({
        "arguments": {"x-dead-letter-routing-key": "value"}
      })
        response = http.put("/api/queues/%2f/precond-failed", body: body)
        response.status_code.should eq 400
      end
    end

    it "should require config access to declare" do
      with_http_server do |http, _|
        hdrs = HTTP::Headers{"Authorization" => "Basic dGVzdF9wZXJtOnB3"}
        response = http.put("/api/queues/%2f/test_perm", headers: hdrs, body: %({}))
        response.status_code.should eq 401
      end
    end
  end
  describe "DELETE /api/queues/vhost/name" do
    it "should delete queue" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("delq", false, false)
        response = http.delete("/api/queues/%2f/delq")
        response.status_code.should eq 204
      end
    end

    it "should not delete queue if it has messasge when query param if-unused is set" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
          q.publish "m1"
          sleep 0.05
          body = %({
          "count": 1,
          "ack_mode": "reject_requeue_true",
          "encoding": "auto"
        })
          response = http.post("/api/queues/%2f/q3/get", body: body)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
          keys = ["payload_bytes", "redelivered", "exchange", "routing_key", "message_count",
                  "properties", "payload", "payload_encoding"]
          body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
          s.vhosts["/"].queues["q3"].message_count.should be > 0
        end
      end
    end
  end
  describe "POST /api/queues/vhost/name/get" do
    it "should get plain text messages" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q4")
          q4 = s.vhosts["/"].queues["q4"]
          q.publish "m1"
          wait_for { q4.message_count == 1 }
          body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
          response = http.post("/api/queues/%2f/q4/get", body: body)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body[0]["payload"].should eq "m1"
          q4.empty?.should be_true
        end
      end
    end

    it "should get encoded messages" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q4")
          q4 = s.vhosts["/"].queues["q4"]
          mem_io = IO::Memory.new
          Compress::Deflate::Writer.open(mem_io, Compress::Deflate::BEST_SPEED) { |deflate| deflate.print("m1") }
          encoded_msg = mem_io.to_s
          q.publish encoded_msg, props: AMQP::Client::Properties.new(content_encoding: "deflate")
          wait_for { q4.message_count == 1 }
          body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
          response = http.post("/api/queues/%2f/q4/get", body: body)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body[0]["payload"].should eq Base64.urlsafe_encode(encoded_msg)
          body[0]["payload_encoding"].should eq "base64"
          q4.empty?.should be_true
        end
      end
    end

    it "should handle count > message_count" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
          q.publish "m1"
          sleep 0.05
          body = %({
          "count": 2,
          "ack_mode": "get",
          "encoding": "auto"
        })
          response = http.post("/api/queues/%2f/q5/get", body: body)
          response.status_code.should eq 200
        end
      end
    end

    it "should handle empty q" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("q6")
          body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "auto"
        })
          response = http.post("/api/queues/%2f/q6/get", body: body)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.should be_empty
        end
      end
    end

    it "should handle base64 encoding" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q7")
          q.publish "m1"
          sleep 0.05
          body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "base64"
        })
          response = http.post("/api/queues/%2f/q7/get", body: body)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          Base64.decode_string(body[0]["payload"].as_s).should eq "m1"
        end
      end
    end

    it "should not allow get and requeue" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("q8")
          q.publish "m1"
          sleep 0.05
          body = %({
          "count": 1,
          "ack_mode": "get",
          "requeue": true,
          "encoding": "base64"
        })
          response = http.post("/api/queues/%2f/q8/get", body: body)
          response.status_code.should eq 400
          body = JSON.parse(response.body)
          body["reason"].should eq "Cannot requeue message on get"
        end
      end
    end
  end
  describe "PUT /api/queues/vhost/name/pause" do
    it "should pause the queue" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("confqueue")
          response = http.put("/api/queues/%2f/confqueue/pause")
          response.status_code.should eq 204
          response = http.get("/api/queues/%2f/confqueue")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["state"].should eq "paused"
        end
      end
    end
  end
  describe "PUT /api/queues/vhost/name/resume" do
    it "should resume the queue" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.queue("confqueue")

          q = s.vhosts["/"].queues["confqueue"]
          q.pause!

          response = http.get("/api/queues/%2f/confqueue")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["state"].should eq "paused"

          response = http.put("/api/queues/%2f/confqueue/resume")
          response.status_code.should eq 204

          response = http.get("/api/queues/%2f/confqueue")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["state"].should eq "running"
        end
      end
    end
  end
end
