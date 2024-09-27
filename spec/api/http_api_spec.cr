require "../spec_helper"

describe LavinMQ::HTTP::Server do
  describe "GET /api/overview" do
    it "should refuse access if no basic auth header" do
      with_http_server do |http, _|
        response = ::HTTP::Client.get("#{http.addr}/api/overview")
        response.status_code.should eq 401
      end
    end

    it "should refuse access if user does not exist" do
      with_http_server do |http, s|
        s.users.delete("arnold")
        # arnold:pw
        response = http.get("/api/overview",
          headers: ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"})
        response.status_code.should eq 401
      end
    end

    it "should refuse access if password does not match" do
      with_http_server do |http, _|
        # guest:pw
        response = http.get("/api/overview",
          headers: ::HTTP::Headers{"Authorization" => "Basic Z3Vlc3Q6cHc="})
        response.status_code.should eq 401
      end
    end

    it "should allow access if user is correct" do
      with_http_server do |http, _|
        response = http.get("/api/overview")
        response.status_code.should eq 200
      end
    end

    it "should filter stats if x-vhost header is set" do
      with_http_server do |http, _|
        response = http.get("/api/whoami")
        response.status_code.should eq 200
      end
    end

    it "should return sum of all published messages" do
      with_http_server do |http, s|
        response = http.get("/api/overview")
        before_count = JSON.parse(response.body).dig("message_stats", "publish")

        with_channel(s) do |ch|
          q1 = ch.queue("stats_q1", exclusive: true)
          q2 = ch.queue("stats_q2", exclusive: true)
          5.times do
            q1.publish_confirm "m"
            q2.publish_confirm "m"
          end
        end

        response = http.get("/api/overview")
        count = JSON.parse(response.body).dig("message_stats", "publish")
        count.should eq(before_count.as_i + 10)
      end
    end

    it "should return the number of published messages" do
      with_http_server do |http, s|
        response = http.get("/api/overview")
        before_count = JSON.parse(response.body).dig("message_stats", "publish")

        with_channel(s) do |ch|
          x = ch.fanout_exchange
          q1 = ch.queue("stats_q1", exclusive: true)
          q2 = ch.queue("stats_q2", exclusive: true)
          q3 = ch.queue("stats_q3", exclusive: true)
          ch.queue_bind(q1.name, x.name, "#")
          ch.queue_bind(q2.name, x.name, "#")
          ch.queue_bind(q3.name, x.name, "#")
          5.times do
            x.publish_confirm("m", "stats")
          end
        end

        response = http.get("/api/overview")
        count = JSON.parse(response.body).dig("message_stats", "publish")
        count.should eq(before_count.as_i + 5)
      end
    end

    it "should return the number of acked and delivered messages" do
      with_http_server do |http, s|
        response = http.get("/api/overview")
        before_ack_count = JSON.parse(response.body).dig("message_stats", "ack")
        before_deliver_count = JSON.parse(response.body).dig("message_stats", "deliver")
        before_deliver_get_count = JSON.parse(response.body).dig("message_stats", "deliver_get")

        with_channel(s) do |ch|
          q1 = ch.queue("stats_q1", exclusive: true)
          5.times do
            q1.publish_confirm("m")
          end
          c = 0
          q1.subscribe(no_ack: false) do |msg|
            ch.basic_ack(msg.delivery_tag)
            c += 1
          end
          wait_for { c == 5 }
        end

        response = http.get("/api/overview")
        count = JSON.parse(response.body).dig("message_stats", "ack")
        count.should eq(before_ack_count.as_i + 5)
        count = JSON.parse(response.body).dig("message_stats", "deliver")
        count.should eq(before_deliver_count.as_i + 5)
        count = JSON.parse(response.body).dig("message_stats", "deliver_get")
        count.should eq(before_deliver_get_count.as_i + 5)
      end
    end

    it "should return the number of rejected and redelivered messages" do
      with_http_server do |http, s|
        response = http.get("/api/overview")
        before_redeliver_count = JSON.parse(response.body).dig("message_stats", "redeliver")
        before_reject_count = JSON.parse(response.body).dig("message_stats", "reject")
        rejected = false

        with_channel(s) do |ch|
          q1 = ch.queue("stats_q1", exclusive: true)
          q1.publish_confirm("m")
          q1.subscribe(no_ack: false) do |msg|
            msg.reject(requeue: true) unless rejected
            msg.ack if rejected
            rejected = true
          end
          wait_for { ch.queue_declare("stats_q1", passive: true)[:message_count] == 0 }
        end

        response = http.get("/api/overview")
        count = JSON.parse(response.body).dig("message_stats", "redeliver")
        count.should eq(before_redeliver_count.as_i + 1)
        count = JSON.parse(response.body).dig("message_stats", "reject")
        count.should eq(before_reject_count.as_i + 1)
      end
    end

    it "should return the number of message gets" do
      with_http_server do |http, s|
        response = http.get("/api/overview")
        before_count = JSON.parse(response.body).dig("message_stats", "get")
        before_count_deliver_get = JSON.parse(response.body).dig("message_stats", "deliver_get")

        with_channel(s) do |ch|
          q1 = ch.queue("stats_q1", exclusive: true)
          5.times do
            q1.publish_confirm("m")
          end
          5.times do
            q1.get.not_nil!
          end
        end

        response = http.get("/api/overview")
        count = JSON.parse(response.body).dig("message_stats", "get")
        count.should eq(before_count.as_i + 5)
        count = JSON.parse(response.body).dig("message_stats", "deliver_get")
        count.should eq(before_count_deliver_get.as_i + 5)
      end
    end
  end

  it "should return the number of message deliver_gets" do
    with_http_server do |http, s|
      response = http.get("/api/overview")
      before_count = JSON.parse(response.body).dig("message_stats", "deliver_get")

      with_channel(s) do |ch|
        q1 = ch.queue("stats_q1", exclusive: true)
        10.times do
          q1.publish_confirm("m")
        end
        5.times do
          q1.get.not_nil!
        end
        c = 0
      end

      response = http.get("/api/overview")
      count = JSON.parse(response.body).dig("message_stats", "deliver_get")
      count.should eq(before_count.as_i + 5)
    end
  end

  describe "GET /api/aliveness-test/vhost" do
    it "should run aliveness-test" do
      with_http_server do |http, _|
        response = http.get("/api/aliveness-test/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["status"].as_s.should eq "ok"
      end
    end
  end

  describe "Pagination" do
    it "should page results" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts?page=1&page_size=1")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["filtered_count", "items", "item_count", "page", "page_size", "total_count"]
        keys.each { |k| body.as_h.keys.should contain(k) }
      end
    end
  end

  describe "Sorting" do
    it "should sort results" do
      with_http_server do |http, s|
        s.vhosts.create("x-vhost")
        s.vhosts.create("a-vhost")
        response = http.get("/api/vhosts?page=1&sort=name")
        response.status_code.should eq 200
        items = JSON.parse(response.body).as_h["items"].as_a
        items.first["name"].should eq "/"
        items.last["name"].should eq "x-vhost"
      end
    end

    it "should sort reverse results" do
      with_http_server do |http, s|
        s.vhosts.create("a-vhost")
        s.vhosts.create("x-vhost")
        response = http.get("/api/vhosts?page=1&sort=name&sort_reverse=true")
        response.status_code.should eq 200
        items = JSON.parse(response.body).as_h["items"].as_a
        items.first["name"].should eq "x-vhost"
        items.last["name"].should eq "/"
      end
    end
  end
end
