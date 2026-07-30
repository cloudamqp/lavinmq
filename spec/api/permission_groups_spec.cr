require "../spec_helper"

describe LavinMQ::HTTP::PermissionGroupsController do
  it "creates, lists, gets and deletes a permission group" do
    with_http_server do |http, _|
      body = {
        protocol: "mqtt",
        members:  ["alice"],
        rules:    [{pattern: "chat/{client_id}/#", read: true, write: true}],
      }.to_json
      response = http.put("/api/permission-groups/chat", body: body)
      response.status_code.should eq 201

      list = http.get("/api/permission-groups")
      list.status_code.should eq 200
      JSON.parse(list.body).as_a.map(&.["name"]).should contain "chat"

      get_one = http.get("/api/permission-groups/chat")
      get_one.status_code.should eq 200
      JSON.parse(get_one.body)["name"].as_s.should eq "chat"

      del = http.delete("/api/permission-groups/chat")
      del.status_code.should eq 204

      after_delete = http.get("/api/permission-groups/chat")
      after_delete.status_code.should eq 404
    end
  end

  it "returns 204 when updating an existing group" do
    with_http_server do |http, s|
      group = LavinMQ::Auth::PermissionGroup.new("grp", "mqtt", [] of String, [] of LavinMQ::Auth::PermissionGroup::Rule)
      s.permission_service.put(group)

      body = {protocol: "mqtt", members: ["*"], rules: [] of NamedTuple(pattern: String, read: Bool, write: Bool)}.to_json
      response = http.put("/api/permission-groups/grp", body: body)
      response.status_code.should eq 204
    end
  end

  it "rejects unsupported protocols and does not create the group" do
    with_http_server do |http, _|
      body = {
        protocol: "amqp",
        members:  ["alice"],
        rules:    [{pattern: "chat/#", read: true, write: true}],
      }.to_json
      response = http.put("/api/permission-groups/bad", body: body)
      response.status_code.should eq 400

      get_one = http.get("/api/permission-groups/bad")
      get_one.status_code.should eq 404
    end
  end

  it "refuses non-administrators on every route" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"} # arnold:pw
      http.get("/api/permission-groups", headers: hdrs).status_code.should eq 403
      http.get("/api/permission-groups/anything", headers: hdrs).status_code.should eq 403
      http.put("/api/permission-groups/foo", headers: hdrs, body: "{}").status_code.should eq 403
      http.delete("/api/permission-groups/anything", headers: hdrs).status_code.should eq 403
    end
  end

  it "returns 400 for wrong field types or a missing pattern, creating nothing" do
    with_http_server do |http, _|
      [
        %({"members": "alice"}),
        %({"members": [1, 2]}),
        %({"rules": {"pattern": "a/#"}}),
        %({"rules": [{"read": true}]}),
        %({"rules": [{"pattern": "a/#", "read": "true"}]}),
        %({"protocol": 1}),
      ].each do |body|
        http.put("/api/permission-groups/bad", body: body).status_code.should eq 400
      end
      http.get("/api/permission-groups/bad").status_code.should eq 404
    end
  end

  it "returns 400 for a malformed topic-filter pattern and does not create the group" do
    with_http_server do |http, _|
      body = {
        protocol: "mqtt",
        members:  ["alice"],
        rules:    [{pattern: "secret/#/temp", read: true, write: false}],
      }.to_json
      response = http.put("/api/permission-groups/bad-pattern", body: body)
      response.status_code.should eq 400

      http.get("/api/permission-groups/bad-pattern").status_code.should eq 404
    end
  end

  it "returns 404 when deleting a group that does not exist" do
    with_http_server do |http, _|
      response = http.delete("/api/permission-groups/does-not-exist")
      response.status_code.should eq 404
    end
  end

  it "accepts absent optional fields and applies their defaults" do
    with_http_server do |http, _|
      body = %({})
      response = http.put("/api/permission-groups/defaults", body: body)
      response.status_code.should eq 201

      group = JSON.parse(http.get("/api/permission-groups/defaults").body)
      group["protocol"].as_s.should eq "mqtt"
      group["members"].as_a.should be_empty
      group["rules"].as_a.should be_empty
    end
  end
end
