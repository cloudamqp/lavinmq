require "../spec_helper"

describe LavinMQ::HTTP::PermissionGroupsController do
  describe "groups" do
    it "creates, lists, gets and deletes a permission group" do
      with_http_server do |http, _|
        response = http.put("/api/permission-groups/%2f/chat")
        response.status_code.should eq 201

        list = http.get("/api/permission-groups")
        list.status_code.should eq 200
        JSON.parse(list.body).as_a.map(&.["name"]).should contain "chat"

        by_vhost = http.get("/api/permission-groups/%2f")
        by_vhost.status_code.should eq 200
        JSON.parse(by_vhost.body).as_a.map(&.["name"]).should contain "chat"

        get_one = http.get("/api/permission-groups/%2f/chat")
        get_one.status_code.should eq 200
        group = JSON.parse(get_one.body)
        group["name"].as_s.should eq "chat"
        group["vhost"].as_s.should eq "/"
        group["members"].as_a.should be_empty
        group["rules"].as_a.should be_empty

        del = http.delete("/api/permission-groups/%2f/chat")
        del.status_code.should eq 204

        http.get("/api/permission-groups/%2f/chat").status_code.should eq 404
      end
    end

    it "returns 204 when creating a group that already exists" do
      with_http_server do |http, _|
        http.put("/api/permission-groups/%2f/grp").status_code.should eq 201
        http.put("/api/permission-groups/%2f/grp").status_code.should eq 204
      end
    end

    it "rejects a create with a body" do
      with_http_server do |http, _|
        body = {members: ["alice"]}.to_json
        http.put("/api/permission-groups/%2f/grp", body: body).status_code.should eq 400
        http.get("/api/permission-groups/%2f/grp").status_code.should eq 404
      end
    end

    it "returns 404 when deleting a group that does not exist" do
      with_http_server do |http, _|
        http.delete("/api/permission-groups/%2f/does-not-exist").status_code.should eq 404
      end
    end
  end

  describe "members" do
    it "adds and removes a member" do
      with_http_server do |http, _|
        http.put("/api/permission-groups/%2f/grp").status_code.should eq 201

        http.put("/api/permission-groups/%2f/grp/members/device-1").status_code.should eq 201
        http.put("/api/permission-groups/%2f/grp/members/device-1").status_code.should eq 204
        http.put("/api/permission-groups/%2f/grp/members/*").status_code.should eq 201

        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["members"].as_a.map(&.as_s).should eq ["device-1", "*"]

        http.delete("/api/permission-groups/%2f/grp/members/device-1").status_code.should eq 204
        http.delete("/api/permission-groups/%2f/grp/members/device-1").status_code.should eq 404

        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["members"].as_a.map(&.as_s).should eq ["*"]
      end
    end

    it "returns 404 for member operations on a missing group" do
      with_http_server do |http, _|
        http.put("/api/permission-groups/%2f/nope/members/m1").status_code.should eq 404
        http.delete("/api/permission-groups/%2f/nope/members/m1").status_code.should eq 404
      end
    end
  end

  describe "rules" do
    it "adds, replaces and removes a rule by identifier" do
      with_http_server do |http, _|
        http.put("/api/permission-groups/%2f/grp").status_code.should eq 201

        rule = {pattern: "chat/{client_id}/#", read: true, write: true}.to_json
        http.put("/api/permission-groups/%2f/grp/rules/own-chat", body: rule).status_code.should eq 201

        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["rules"].as_a.size.should eq 1
        group["rules"][0]["identifier"].as_s.should eq "own-chat"
        group["rules"][0]["write"].as_bool.should be_true

        replace = {pattern: "chat/{client_id}/#", read: true, write: false}.to_json
        http.put("/api/permission-groups/%2f/grp/rules/own-chat", body: replace).status_code.should eq 204

        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["rules"].as_a.size.should eq 1
        group["rules"][0]["write"].as_bool.should be_false

        http.delete("/api/permission-groups/%2f/grp/rules/own-chat").status_code.should eq 204
        http.delete("/api/permission-groups/%2f/grp/rules/own-chat").status_code.should eq 404

        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["rules"].as_a.should be_empty
      end
    end

    it "rejects an invalid rule, creating nothing" do
      with_http_server do |http, _|
        http.put("/api/permission-groups/%2f/grp").status_code.should eq 201
        [
          {path: "ok", body: %({})},                           # missing pattern
          {path: "ok", body: %({"pattern": 1})},               # wrong type
          {path: "ok", body: %({"pattern": "secret/#/temp"})}, # malformed filter
          {path: "not%20ok", body: %({"pattern": "a/#"})},     # invalid identifier
        ].each do |c|
          http.put("/api/permission-groups/%2f/grp/rules/#{c[:path]}", body: c[:body]).status_code.should eq 400
        end
        group = JSON.parse(http.get("/api/permission-groups/%2f/grp").body)
        group["rules"].as_a.should be_empty
      end
    end

    it "returns 404 for rule operations on a missing group" do
      with_http_server do |http, _|
        body = {pattern: "a/#"}.to_json
        http.put("/api/permission-groups/%2f/nope/rules/r1", body: body).status_code.should eq 404
        http.delete("/api/permission-groups/%2f/nope/rules/r1").status_code.should eq 404
      end
    end
  end

  it "refuses non-administrators on every route" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"} # arnold:pw
      http.get("/api/permission-groups", headers: hdrs).status_code.should eq 403
      http.get("/api/permission-groups/%2f", headers: hdrs).status_code.should eq 403
      http.get("/api/permission-groups/%2f/anything", headers: hdrs).status_code.should eq 403
      http.put("/api/permission-groups/%2f/foo", headers: hdrs).status_code.should eq 403
      http.delete("/api/permission-groups/%2f/anything", headers: hdrs).status_code.should eq 403
      http.put("/api/permission-groups/%2f/foo/members/m1", headers: hdrs).status_code.should eq 403
      http.delete("/api/permission-groups/%2f/foo/members/m1", headers: hdrs).status_code.should eq 403
      http.put("/api/permission-groups/%2f/foo/rules/r1", headers: hdrs, body: "{}").status_code.should eq 403
      http.delete("/api/permission-groups/%2f/foo/rules/r1", headers: hdrs).status_code.should eq 403
    end
  end

  it "returns 404 for an unknown vhost" do
    with_http_server do |http, _|
      http.put("/api/permission-groups/nope/grp").status_code.should eq 404
    end
  end
end
