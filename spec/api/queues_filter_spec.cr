require "../spec_helper"

describe "queue filter HTTP API" do
  describe "PUT /api/queues/:vhost/:name/filter" do
    it "creates an auto-managed policy carrying the filter rule" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-http", true, false)
        body = %({
          "clauses":[{"key":"x-test","op":"eq","value":"bad"}],
          "action":"drop",
          "rule_id":"r1"
        })
        response = http.put("/api/queues/%2f/filter-http/filter", body: body)
        response.status_code.should eq 201
        policy = s.vhosts["/"].policies["__queue-filter__filter-http"]?
        policy.should_not be_nil
        policy.not_nil!.pattern.matches?("filter-http").should be_true
        policy.not_nil!.pattern.matches?("filter-http-other").should be_false
        policy.not_nil!.apply_to.should eq LavinMQ::Policy::Target::Queues
        policy.not_nil!.priority.should eq 100
        policy.not_nil!.definition["message-filter"].as_h["action"].as_s.should eq "drop"
      end
    end

    it "returns 204 when updating an existing managed policy" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-update", true, false)
        body1 = %({"clauses":[{"key":"x","op":"exists"}],"action":"drop"})
        http.put("/api/queues/%2f/filter-update/filter", body: body1).status_code.should eq 201
        body2 = %({"clauses":[{"key":"y","op":"exists"}],"action":"drop"})
        response = http.put("/api/queues/%2f/filter-update/filter", body: body2)
        response.status_code.should eq 204
        rule = s.vhosts["/"].policies["__queue-filter__filter-update"].definition["message-filter"].as_h
        rule["clauses"].as_a[0].as_h["key"].as_s.should eq "y"
      end
    end

    it "rejects invalid filter JSON" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-bad", true, false)
        body = %({"clauses": []})
        response = http.put("/api/queues/%2f/filter-bad/filter", body: body)
        response.status_code.should eq 400
        JSON.parse(response.body)["reason"].as_s.should match(/at least one clause/)
      end
    end

    it "rejects move_to without target" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-no-target", true, false)
        body = %({"clauses":[{"key":"x","op":"exists"}],"action":"move_to"})
        response = http.put("/api/queues/%2f/filter-no-target/filter", body: body)
        response.status_code.should eq 400
        JSON.parse(response.body)["reason"].as_s.should match(/target/)
      end
    end

    it "returns 404 for missing queue" do
      with_http_server do |http, _|
        response = http.put("/api/queues/%2f/no-such-queue/filter",
          body: %({"clauses":[{"key":"x","op":"exists"}]}))
        response.status_code.should eq 404
      end
    end
  end

  describe "DELETE /api/queues/:vhost/:name/filter" do
    it "deletes the auto-managed policy" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-del", true, false)
        http.put("/api/queues/%2f/filter-del/filter",
          body: %({"clauses":[{"key":"x","op":"exists"}],"action":"drop"}))
        s.vhosts["/"].policies.has_key?("__queue-filter__filter-del").should be_true
        response = http.delete("/api/queues/%2f/filter-del/filter")
        response.status_code.should eq 204
        s.vhosts["/"].policies.has_key?("__queue-filter__filter-del").should be_false
      end
    end

    it "returns 404 when no managed policy exists" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-none", true, false)
        response = http.delete("/api/queues/%2f/filter-none/filter")
        response.status_code.should eq 404
      end
    end
  end

  describe "GET /api/queues/:vhost/:name/filter" do
    it "reports managed-policy source after PUT" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-src-1", true, false)
        body = %({"clauses":[{"key":"x","op":"exists"}],"action":"drop","rule_id":"a"})
        http.put("/api/queues/%2f/filter-src-1/filter", body: body)
        sleep 20.milliseconds
        response = http.get("/api/queues/%2f/filter-src-1/filter")
        response.status_code.should eq 200
        data = JSON.parse(response.body)
        data["source"].as_s.should eq "managed-policy"
        data["rule"].as_h["rule_id"].as_s.should eq "a"
      end
    end

    it "returns nil source when queue has no filter" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("filter-src-none", true, false)
        response = http.get("/api/queues/%2f/filter-src-none/filter")
        response.status_code.should eq 200
        JSON.parse(response.body)["source"].raw.should be_nil
      end
    end
  end
end
