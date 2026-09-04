require "../spec_helper"

describe LavinMQ::HTTP::PermissionGroupsController do
  describe "groups" do
    it "creates, lists, gets and deletes a permission group" do
      with_http_server do |http, _|
        response = http.put("/api/mqtt/permission-groups/%2f/chat")
        response.status_code.should eq 201

        list = http.get("/api/mqtt/permission-groups")
        list.status_code.should eq 200
        JSON.parse(list.body).as_a.map(&.["name"]).should contain "chat"

        by_vhost = http.get("/api/mqtt/permission-groups/%2f")
        by_vhost.status_code.should eq 200
        JSON.parse(by_vhost.body).as_a.map(&.["name"]).should contain "chat"

        get_one = http.get("/api/mqtt/permission-groups/%2f/chat")
        get_one.status_code.should eq 200
        group = JSON.parse(get_one.body)
        group["name"].as_s.should eq "chat"
        group["vhost"].as_s.should eq "/"
        group["member_count"].as_i.should eq 0
        group["rule_count"].as_i.should eq 0
        group["members"]?.should be_nil
        group["rules"]?.should be_nil

        del = http.delete("/api/mqtt/permission-groups/%2f/chat")
        del.status_code.should eq 204

        http.get("/api/mqtt/permission-groups/%2f/chat").status_code.should eq 404
      end
    end

    it "lists groups as summaries with pagination and filtering" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/chat").status_code.should eq 201
        http.put("/api/mqtt/permission-groups/%2f/chat/members/m1").status_code.should eq 201
        rule = {pattern: "chat/#", read: true}.to_json
        http.put("/api/mqtt/permission-groups/%2f/chat/rules/r1", body: rule).status_code.should eq 201
        http.put("/api/mqtt/permission-groups/%2f/other").status_code.should eq 201

        list = JSON.parse(http.get("/api/mqtt/permission-groups").body).as_a
        chat = list.find! { |g| g["name"] == "chat" }
        chat["vhost"].as_s.should eq "/"
        chat["member_count"].as_i.should eq 1
        chat["rule_count"].as_i.should eq 1
        chat["members"]?.should be_nil
        chat["rules"]?.should be_nil

        paged = JSON.parse(http.get("/api/mqtt/permission-groups/%2f?page=1&page_size=1").body)
        paged["items"].as_a.size.should eq 1
        paged["total_count"].as_i.should eq 2
        paged["page_count"].as_i.should eq 2

        filtered = JSON.parse(http.get("/api/mqtt/permission-groups/%2f?name=chat").body).as_a
        filtered.size.should eq 1
        filtered[0]["name"].as_s.should eq "chat"
      end
    end

    it "rejects an invalid group name, creating nothing" do
      with_http_server do |http, _|
        [
          "a%20b",   # space
          "a%2Fb",   # slash
          "a.b",     # dot
          "%C3%A5",  # non-ascii
          "..",      # path traversal
          "x" * 256, # too long
        ].each do |name|
          http.put("/api/mqtt/permission-groups/%2f/#{name}").status_code.should eq 400
        end
        JSON.parse(http.get("/api/mqtt/permission-groups/%2f").body).as_a.should be_empty
      end
    end

    it "accepts a group name with hyphens and underscores" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/Team_1-iot").status_code.should eq 201
      end
    end

    it "returns 204 when creating a group that already exists" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 201
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 204
      end
    end

    it "rejects a create with a body" do
      with_http_server do |http, _|
        body = {members: ["alice"]}.to_json
        http.put("/api/mqtt/permission-groups/%2f/grp", body: body).status_code.should eq 400
        http.get("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 404
      end
    end

    it "returns 404 when deleting a group that does not exist" do
      with_http_server do |http, _|
        http.delete("/api/mqtt/permission-groups/%2f/does-not-exist").status_code.should eq 404
      end
    end
  end

  describe "members" do
    it "adds, lists and removes members" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 201

        http.put("/api/mqtt/permission-groups/%2f/grp/members/device-1").status_code.should eq 201
        http.put("/api/mqtt/permission-groups/%2f/grp/members/device-1").status_code.should eq 204
        http.put("/api/mqtt/permission-groups/%2f/grp/members/*").status_code.should eq 201

        group = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp").body)
        group["member_count"].as_i.should eq 2
        members = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/members").body).as_a
        members.map(&.["username"].as_s).should eq ["device-1", "*"]

        http.delete("/api/mqtt/permission-groups/%2f/grp/members/device-1").status_code.should eq 204
        http.delete("/api/mqtt/permission-groups/%2f/grp/members/device-1").status_code.should eq 404

        group = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp").body)
        group["member_count"].as_i.should eq 1
        members = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/members").body).as_a
        members.map(&.["username"].as_s).should eq ["*"]
      end
    end

    it "lists members with pagination and filtering" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 201
        3.times do |i|
          http.put("/api/mqtt/permission-groups/%2f/grp/members/device-#{i}").status_code.should eq 201
        end

        paged = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/members?page=2&page_size=2").body)
        paged["items"].as_a.map(&.["username"].as_s).should eq ["device-2"]
        paged["total_count"].as_i.should eq 3
        paged["page_count"].as_i.should eq 2

        filtered = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/members?name=device-1").body).as_a
        filtered.map(&.["username"].as_s).should eq ["device-1"]
      end
    end

    it "returns 404 for member operations on a missing group" do
      with_http_server do |http, _|
        http.get("/api/mqtt/permission-groups/%2f/nope/members").status_code.should eq 404
        http.put("/api/mqtt/permission-groups/%2f/nope/members/m1").status_code.should eq 404
        http.delete("/api/mqtt/permission-groups/%2f/nope/members/m1").status_code.should eq 404
      end
    end
  end

  describe "rules" do
    it "adds, lists, replaces and removes a rule by identifier" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 201

        rule = {pattern: "chat/{client_id}/#", read: true, write: true}.to_json
        http.put("/api/mqtt/permission-groups/%2f/grp/rules/own-chat", body: rule).status_code.should eq 201

        group = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp").body)
        group["rule_count"].as_i.should eq 1
        rules = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/rules").body).as_a
        rules.size.should eq 1
        rules[0]["identifier"].as_s.should eq "own-chat"
        rules[0]["pattern"].as_s.should eq "chat/{client_id}/#"
        rules[0]["read"].as_bool.should be_true
        rules[0]["write"].as_bool.should be_true

        replace = {pattern: "chat/{client_id}/#", read: true, write: false}.to_json
        http.put("/api/mqtt/permission-groups/%2f/grp/rules/own-chat", body: replace).status_code.should eq 204

        rules = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/rules").body).as_a
        rules.size.should eq 1
        rules[0]["write"].as_bool.should be_false

        http.delete("/api/mqtt/permission-groups/%2f/grp/rules/own-chat").status_code.should eq 204
        http.delete("/api/mqtt/permission-groups/%2f/grp/rules/own-chat").status_code.should eq 404

        group = JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp").body)
        group["rule_count"].as_i.should eq 0
        JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/rules").body).as_a.should be_empty
      end
    end

    it "rejects an invalid rule, creating nothing" do
      with_http_server do |http, _|
        http.put("/api/mqtt/permission-groups/%2f/grp").status_code.should eq 201
        [
          {path: "ok", body: %({})},                           # missing pattern
          {path: "ok", body: %({"pattern": 1})},               # wrong type
          {path: "ok", body: %({"pattern": "secret/#/temp"})}, # malformed filter
          {path: "not%20ok", body: %({"pattern": "a/#"})},     # invalid identifier
        ].each do |c|
          http.put("/api/mqtt/permission-groups/%2f/grp/rules/#{c[:path]}", body: c[:body]).status_code.should eq 400
        end
        JSON.parse(http.get("/api/mqtt/permission-groups/%2f/grp/rules").body).as_a.should be_empty
      end
    end

    it "returns 404 for rule operations on a missing group" do
      with_http_server do |http, _|
        body = {pattern: "a/#"}.to_json
        http.get("/api/mqtt/permission-groups/%2f/nope/rules").status_code.should eq 404
        http.put("/api/mqtt/permission-groups/%2f/nope/rules/r1", body: body).status_code.should eq 404
        http.delete("/api/mqtt/permission-groups/%2f/nope/rules/r1").status_code.should eq 404
      end
    end
  end

  it "refuses non-administrators on every route" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"} # arnold:pw
      http.get("/api/mqtt/permission-groups", headers: hdrs).status_code.should eq 403
      http.get("/api/mqtt/permission-groups/%2f", headers: hdrs).status_code.should eq 403
      http.get("/api/mqtt/permission-groups/%2f/anything", headers: hdrs).status_code.should eq 403
      http.put("/api/mqtt/permission-groups/%2f/foo", headers: hdrs).status_code.should eq 403
      http.delete("/api/mqtt/permission-groups/%2f/anything", headers: hdrs).status_code.should eq 403
      http.get("/api/mqtt/permission-groups/%2f/foo/members", headers: hdrs).status_code.should eq 403
      http.put("/api/mqtt/permission-groups/%2f/foo/members/m1", headers: hdrs).status_code.should eq 403
      http.delete("/api/mqtt/permission-groups/%2f/foo/members/m1", headers: hdrs).status_code.should eq 403
      http.get("/api/mqtt/permission-groups/%2f/foo/rules", headers: hdrs).status_code.should eq 403
      http.put("/api/mqtt/permission-groups/%2f/foo/rules/r1", headers: hdrs, body: "{}").status_code.should eq 403
      http.delete("/api/mqtt/permission-groups/%2f/foo/rules/r1", headers: hdrs).status_code.should eq 403
    end
  end

  it "returns 404 for an unknown vhost" do
    with_http_server do |http, _|
      http.put("/api/mqtt/permission-groups/nope/grp").status_code.should eq 404
    end
  end
end
