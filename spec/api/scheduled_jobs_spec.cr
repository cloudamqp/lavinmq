require "../spec_helper"
require "uri"

private def put_job(http, vhost, name, value)
  http.put("/api/parameters/scheduled-job/#{URI.encode_path_segment(vhost)}/#{URI.encode_path_segment(name)}",
    body: {value: value}.to_json)
end

describe LavinMQ::HTTP::ScheduledJobsController do
  describe "PUT /api/parameters/scheduled-job/:vhost/:name (validation)" do
    it "creates a job and returns 201" do
      with_http_server do |http, s|
        response = put_job(http, "/", "create-201", {
          cron:          "*/5 * * * *",
          exchange:      "",
          "routing-key": "noop",
          body:          "{}",
        })
        response.status_code.should eq 201
        s.vhosts["/"].scheduled_jobs.has_key?("create-201").should be_true
        s.vhosts["/"].delete_parameter("scheduled-job", "create-201")
      end
    end

    it "returns 400 for an invalid cron" do
      with_http_server do |http, _s|
        response = put_job(http, "/", "bad-cron", {
          cron:          "not-a-cron",
          "routing-key": "noop",
          body:          "{}",
        })
        response.status_code.should eq 400
        response.body.includes?("cron").should be_true
      end
    end

    it "returns 400 for a missing routing-key" do
      with_http_server do |http, _s|
        response = put_job(http, "/", "no-rk", {
          cron: "* * * * *",
          body: "{}",
        })
        response.status_code.should eq 400
      end
    end

    it "accepts the alternate 'schedule' and 'routing_key' field names" do
      with_http_server do |http, s|
        response = put_job(http, "/", "alt-names", {
          schedule:    "0 9 * * 1-5",
          exchange:    "amq.default",
          routing_key: "any",
          body:        "{}",
        })
        response.status_code.should eq 201
        s.vhosts["/"].scheduled_jobs["alt-names"].exchange.should eq ""
        s.vhosts["/"].delete_parameter("scheduled-job", "alt-names")
      end
    end
  end

  describe "GET /api/scheduled-jobs" do
    it "lists scheduled jobs across vhosts" do
      with_http_server do |http, s|
        put_job(http, "/", "list-1", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        response = http.get("/api/scheduled-jobs")
        response.status_code.should eq 200
        names = JSON.parse(response.body).as_a.map(&.["name"].as_s)
        names.includes?("list-1").should be_true
        s.vhosts["/"].delete_parameter("scheduled-job", "list-1")
      end
    end

    it "lists scheduled jobs for a single vhost" do
      with_http_server do |http, s|
        put_job(http, "/", "list-vh", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        response = http.get("/api/scheduled-jobs/#{URI.encode_path_segment("/")}")
        response.status_code.should eq 200
        names = JSON.parse(response.body).as_a.map(&.["name"].as_s)
        names.should eq ["list-vh"]
        s.vhosts["/"].delete_parameter("scheduled-job", "list-vh")
      end
    end
  end

  describe "GET /api/scheduled-jobs/:vhost/:name" do
    it "returns 404 for a missing job" do
      with_http_server do |http, _s|
        response = http.get("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/missing")
        response.status_code.should eq 404
      end
    end

    it "returns the job's details_tuple" do
      with_http_server do |http, s|
        put_job(http, "/", "details", {
          cron: "*/30 * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        response = http.get("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/details")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["name"].as_s.should eq "details"
        body["cron"].as_s.should eq "*/30 * * * *"
        body["state"].as_s.should eq "Scheduled"
        body["run_count"].as_i.should eq 0
        s.vhosts["/"].delete_parameter("scheduled-job", "details")
      end
    end
  end

  describe "PUT /api/scheduled-jobs/:vhost/:name/pause" do
    it "returns 204 on success" do
      with_http_server do |http, s|
        put_job(http, "/", "pause-ok", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        response = http.put("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/pause-ok/pause")
        response.status_code.should eq 204
        s.vhosts["/"].scheduled_jobs["pause-ok"].state.should eq LavinMQ::ScheduledJob::Runner::State::Paused
        s.vhosts["/"].delete_parameter("scheduled-job", "pause-ok")
      end
    end

    it "returns 422 if already paused" do
      with_http_server do |http, s|
        put_job(http, "/", "pause-422", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        s.vhosts["/"].scheduled_jobs["pause-422"].pause
        response = http.put("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/pause-422/pause")
        response.status_code.should eq 422
        s.vhosts["/"].delete_parameter("scheduled-job", "pause-422")
      end
    end

    it "returns 404 for missing" do
      with_http_server do |http, _s|
        response = http.put("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/no-such/pause")
        response.status_code.should eq 404
      end
    end
  end

  describe "PUT /api/scheduled-jobs/:vhost/:name/resume" do
    it "returns 204 on success" do
      with_http_server do |http, s|
        put_job(http, "/", "resume-ok", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        s.vhosts["/"].scheduled_jobs["resume-ok"].pause
        response = http.put("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/resume-ok/resume")
        response.status_code.should eq 204
        s.vhosts["/"].scheduled_jobs["resume-ok"].state.should eq LavinMQ::ScheduledJob::Runner::State::Scheduled
        s.vhosts["/"].delete_parameter("scheduled-job", "resume-ok")
      end
    end

    it "returns 422 if not paused" do
      with_http_server do |http, s|
        put_job(http, "/", "resume-422", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        response = http.put("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/resume-422/resume")
        response.status_code.should eq 422
        s.vhosts["/"].delete_parameter("scheduled-job", "resume-422")
      end
    end
  end

  describe "POST /api/scheduled-jobs/:vhost/:name/run" do
    it "fires the job and returns 202" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("run-target", durable: false, auto_delete: false)
        s.vhosts["/"].bind_queue("run-target", "amq.topic", "run.target")
        put_job(http, "/", "run-now", {
          cron:          "0 0 1 1 *",
          exchange:      "amq.topic",
          "routing-key": "run.target",
          body:          "hi",
        }).status_code.should eq 201
        response = http.post("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/run-now/run")
        response.status_code.should eq 202
        wait_for { s.vhosts["/"].scheduled_jobs["run-now"].run_count > 0 }
        s.vhosts["/"].queue("run-target").message_count.should eq 1
        s.vhosts["/"].delete_parameter("scheduled-job", "run-now")
      end
    end

    it "returns 422 when paused" do
      with_http_server do |http, s|
        put_job(http, "/", "run-paused", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        s.vhosts["/"].scheduled_jobs["run-paused"].pause
        response = http.post("/api/scheduled-jobs/#{URI.encode_path_segment("/")}/run-paused/run")
        response.status_code.should eq 422
        s.vhosts["/"].delete_parameter("scheduled-job", "run-paused")
      end
    end
  end

  describe "DELETE via /api/parameters/scheduled-job/:vhost/:name" do
    it "removes the job and the runner" do
      with_http_server do |http, s|
        put_job(http, "/", "del-me", {
          cron: "* * * * *", "routing-key": "k", body: "b",
        }).status_code.should eq 201
        s.vhosts["/"].scheduled_jobs.has_key?("del-me").should be_true
        response = http.delete("/api/parameters/scheduled-job/#{URI.encode_path_segment("/")}/del-me")
        response.status_code.should eq 204
        s.vhosts["/"].scheduled_jobs.has_key?("del-me").should be_false
      end
    end
  end
end
