require "../spec_helper"

describe LavinMQ::HTTP::PermissionGroupsController do
  it "creates, lists, gets and deletes a permission group" do
    with_http_server do |http, _|
      body = {
        protocol:     "mqtt",
        apply_to_all: false,
        members:      ["alice"],
        rules:        [{pattern: "chat/{client_id}/#", read: true, write: true}],
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
      group = LavinMQ::Auth::PermissionGroup.new("grp", "mqtt", false, [] of String, [] of LavinMQ::Auth::PermissionGroup::Rule)
      s.permission_groups.put(group)

      body = {protocol: "mqtt", apply_to_all: true, members: [] of String, rules: [] of NamedTuple(pattern: String, read: Bool, write: Bool)}.to_json
      response = http.put("/api/permission-groups/grp", body: body)
      response.status_code.should eq 204
    end
  end

  it "rejects unsupported protocols and does not create the group" do
    with_http_server do |http, _|
      body = {
        protocol:     "amqp",
        apply_to_all: false,
        members:      ["alice"],
        rules:        [{pattern: "chat/#", read: true, write: true}],
      }.to_json
      response = http.put("/api/permission-groups/bad", body: body)
      response.status_code.should eq 400

      get_one = http.get("/api/permission-groups/bad")
      get_one.status_code.should eq 404
    end
  end

  it "refuses non-administrators on list" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"} # arnold:pw
      response = http.get("/api/permission-groups", headers: hdrs)
      response.status_code.should eq 403
    end
  end

  it "refuses non-administrators on put" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"} # arnold:pw
      response = http.put("/api/permission-groups/foo", headers: hdrs, body: "{}")
      response.status_code.should eq 403
    end
  end
end
