require "../spec_helper"

describe "message-filter in /api/policies" do
  it "accepts a JSON object value" do
    with_http_server do |http, s|
      body = %({
        "apply-to": "queues",
        "priority": 5,
        "pattern": ".*",
        "definition": {
          "message-filter": {
            "clauses": [{"key":"x-test","op":"eq","value":"bad"}],
            "action": "drop"
          }
        }
      })
      response = http.put("/api/policies/%2f/mfp", body: body)
      response.status_code.should eq 201
      s.vhosts["/"].policies["mfp"].definition["message-filter"].as_h["action"].as_s.should eq "drop"
    end
  end

  it "rejects empty clauses" do
    with_http_server do |http, _|
      body = %({
        "apply-to": "queues",
        "priority": 5,
        "pattern": ".*",
        "definition": { "message-filter": { "clauses": [] } }
      })
      response = http.put("/api/policies/%2f/mfp-bad", body: body)
      response.status_code.should eq 400
      JSON.parse(response.body)["reason"].as_s.should match(/at least one clause/)
    end
  end

  it "rejects non-object value" do
    with_http_server do |http, _|
      body = %({
        "apply-to": "queues",
        "priority": 5,
        "pattern": ".*",
        "definition": { "message-filter": "not-an-object" }
      })
      response = http.put("/api/policies/%2f/mfp-bad2", body: body)
      response.status_code.should eq 400
      JSON.parse(response.body)["reason"].as_s.should match(/JSON object/)
    end
  end
end
