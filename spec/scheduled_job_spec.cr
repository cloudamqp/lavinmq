require "./spec_helper"

describe LavinMQ::ScheduledJob do
  describe "via HTTP API" do
    it "can create a scheduled job" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("test-queue")
          ch.queue_bind(q.name, "amq.topic", "test.key")
        end

        body = %({
          "value": {
            "exchange": "amq.topic",
            "routing_key": "test.key",
            "body": "{\\"message\\":\\"hello\\"}",
            "schedule": "*/5 * * * *",
            "enabled": true
          }
        })
        response = http.put("/api/parameters/scheduled-job/%2F/test-job", body: body)
        response.status_code.should eq 201

        # Verify it was created
        response = http.get("/api/parameters/scheduled-job/%2F/test-job")
        response.status_code.should eq 200
        json = JSON.parse(response.body)
        json["name"].should eq "test-job"
        json["value"]["exchange"].should eq "amq.topic"
        json["value"]["routing_key"].should eq "test.key"
        json["value"]["schedule"].should eq "*/5 * * * *"
      end
    end

    it "can list scheduled jobs" do
      with_http_server do |http, s|
        body = %({
          "value": {
            "exchange": "amq.topic",
            "routing_key": "test.key",
            "body": "{\\"message\\":\\"hello\\"}",
            "schedule": "* * * * *",
            "enabled": true
          }
        })
        http.put("/api/parameters/scheduled-job/%2F/test-job-1", body: body)
        http.put("/api/parameters/scheduled-job/%2F/test-job-2", body: body)

        sleep 0.1.seconds # Give jobs time to start

        response = http.get("/api/scheduled-jobs/%2F")
        response.status_code.should eq 200
        jobs = Array(JSON::Any).from_json(response.body)
        jobs.size.should eq 2
        jobs.map { |j| j["name"].as_s }.sort.should eq ["test-job-1", "test-job-2"]
      end
    end

    it "can delete a scheduled job" do
      with_http_server do |http, s|
        body = %({
          "value": {
            "exchange": "amq.topic",
            "routing_key": "test.key",
            "body": "{\\"message\\":\\"hello\\"}",
            "schedule": "* * * * *",
            "enabled": true
          }
        })
        http.put("/api/parameters/scheduled-job/%2F/test-job", body: body)

        response = http.delete("/api/parameters/scheduled-job/%2F/test-job")
        response.status_code.should eq 204

        response = http.get("/api/parameters/scheduled-job/%2F/test-job")
        response.status_code.should eq 404
      end
    end

    it "can enable/disable a scheduled job" do
      with_http_server do |http, s|
        body = %({
          "value": {
            "exchange": "amq.topic",
            "routing_key": "test.key",
            "body": "{\\"message\\":\\"hello\\"}",
            "schedule": "* * * * *",
            "enabled": true
          }
        })
        http.put("/api/parameters/scheduled-job/%2F/test-job", body: body)

        sleep 0.1.seconds # Give job time to start

        # Disable
        response = http.put("/api/scheduled-jobs/%2F/test-job/disable", body: nil)
        response.status_code.should eq 204

        # Verify disabled
        response = http.get("/api/scheduled-jobs/%2F/test-job")
        response.status_code.should eq 200
        json = JSON.parse(response.body)
        json["enabled"].should eq false

        # Enable
        response = http.put("/api/scheduled-jobs/%2F/test-job/enable", body: nil)
        response.status_code.should eq 204

        # Verify enabled
        response = http.get("/api/scheduled-jobs/%2F/test-job")
        response.status_code.should eq 200
        json = JSON.parse(response.body)
        json["enabled"].should eq true
      end
    end

    it "can trigger a scheduled job manually" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("trigger-test-queue")
          ch.queue_bind(q.name, "amq.topic", "trigger.test")

          body = %({
            "value": {
              "exchange": "amq.topic",
              "routing_key": "trigger.test",
              "body": "{\\"triggered\\":\\"manually\\"}",
              "schedule": "0 0 1 1 *",
              "enabled": true
            }
          })
          http.put("/api/parameters/scheduled-job/%2F/trigger-test", body: body)

          sleep 0.1.seconds # Give job time to start

          # Get initial last_run (should be nil)
          response = http.get("/api/scheduled-jobs/%2F/trigger-test")
          response.status_code.should eq 200
          json = JSON.parse(response.body)
          json["last_run"].should be_nil

          # Trigger the job manually
          response = http.put("/api/scheduled-jobs/%2F/trigger-test/trigger", body: nil)
          response.status_code.should eq 204

          sleep 0.1.seconds # Give time for message to be published

          # Verify last_run was updated
          response = http.get("/api/scheduled-jobs/%2F/trigger-test")
          response.status_code.should eq 200
          json = JSON.parse(response.body)
          json["last_run"].should_not be_nil

          # Verify message was published to queue
          msg = q.get(no_ack: true)
          msg.should_not be_nil
          msg.try(&.body_io.to_s).should eq %({\"triggered\":\"manually\"})
        end
      end
    end

    it "verifies job has proper state" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("test-queue")
          ch.queue_bind(q.name, "amq.topic", "test.key")

          body = %({
            "value": {
              "exchange": "amq.topic",
              "routing_key": "test.key",
              "body": "{\\"message\\":\\"scheduled\\"}",
              "schedule": "* * * * *",
              "enabled": true
            }
          })
          http.put("/api/parameters/scheduled-job/%2F/test-job", body: body)

          sleep 0.2.seconds # Give job time to initialize

          response = http.get("/api/scheduled-jobs/%2F/test-job")
          response.status_code.should eq 200
          json = JSON.parse(response.body)
          json["state"].should eq "running"
          json["next_run"].should_not be_nil
        end
      end
    end
  end

  describe "ScheduledJob::Runner" do
    it "calculates next run time on initialization" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.exchange_declare("test-exchange", "topic")
        end

        job = LavinMQ::ScheduledJob::Runner.new(
          "test-job",
          vhost,
          "test-exchange",
          "test.key",
          %({\"message\":\"test\"}),
          "* * * * *",
          true
        )

        sleep 0.1.seconds # Give job time to calculate next run

        job.next_run.should_not be_nil
        job.state.should eq LavinMQ::ScheduledJob::State::Running
      ensure
        job.try &.close
      end
    end

    it "can be disabled and enabled" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.exchange_declare("test-exchange", "topic")
        end

        job = LavinMQ::ScheduledJob::Runner.new(
          "test-job",
          vhost,
          "test-exchange",
          "test.key",
          %({\"message\":\"test\"}),
          "* * * * *",
          true
        )

        sleep 0.1.seconds

        job.enabled?.should be_true

        job.disable
        job.enabled?.should be_false

        job.enable
        job.enabled?.should be_true
      ensure
        job.try &.close
      end
    end

    it "handles invalid CRON expressions gracefully" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]

        expect_raises(Exception) do
          LavinMQ::ScheduledJob::Runner.new(
            "test-job",
            vhost,
            "test-exchange",
            "test.key",
            %({\"message\":\"test\"}),
            "invalid cron",
            true
          )
        end
      end
    end
  end
end
