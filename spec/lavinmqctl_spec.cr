require "./spec_helper"
require "../src/lavinmqctl/cli"

# Helper to run lavinmqctl commands against test server
def run_lavinmqctl(http_addr : String, argv : Array(String))
  stdout_capture = IO::Memory.new
  stderr_capture = IO::Memory.new
  stderr = ""
  exit_code = 0

  original_argv = ARGV.dup
  begin
    ARGV.clear
    ARGV.concat(["--uri", "http://#{http_addr}", "--user", "guest", "--password", "guest"]).concat(argv)

    cli = LavinMQCtl.new(stdout_capture, stderr_capture)
    cli.run_cmd
  rescue ex
    stderr = ex.message.to_s
    exit_code = 1
  ensure
    ARGV.clear
    ARGV.concat(original_argv)
  end

  {
    stdout: stdout_capture.to_s,
    stderr: stderr_capture.to_s + stderr,
    exit:   exit_code,
  }
end

describe "LavinMQCtl" do
  describe "with http server" do
    it "should list users" do
      with_http_server do |(http, s)|
        s.users.create("test_user", "password", [LavinMQ::Tag::Administrator])
        result = run_lavinmqctl(http.addr.to_s, ["list_users"])
        result[:exit].should eq(0)
        result[:stdout].should contain("guest")
        result[:stdout].should contain("test_user")
      end
    end

    it "should list users in JSON format" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_users", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_a?.should_not be_nil
        json.as_a.any? { |u| u.as_h["name"] == "guest" }.should be_true
      end
    end

    it "should list queues" do
      with_http_server do |(http, s)|
        vhost = s.vhosts["/"]
        vhost.declare_queue("test_queue", true, false)
        result = run_lavinmqctl(http.addr.to_s, ["list_queues"])
        result[:exit].should eq(0)
        result[:stdout].should contain("test_queue")
      end
    end

    it "should list queues in JSON format" do
      with_http_server do |(http, s)|
        vhost = s.vhosts["/"]
        vhost.declare_queue("test_queue", true, false)
        result = run_lavinmqctl(http.addr.to_s, ["list_queues", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_a?.should_not be_nil
        json.as_a.any? { |q| q.as_h["name"] == "test_queue" }.should be_true
      end
    end

    it "should list vhosts" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_vhosts"])
        result[:exit].should eq(0)
        result[:stdout].should contain("/")
      end
    end

    it "should show status" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["status"])
        result[:exit].should eq(0)
        result[:stdout].should contain("Version")
        result[:stdout].should contain("Connections")
        result[:stdout].should contain("Queues")
        result[:stdout].should contain("Bindings")
      end
    end

    it "should show status in JSON format" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["status", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_h?.should_not be_nil
        json.as_h.has_key?("Version").should be_true
        json.as_h.has_key?("Queues").should be_true
        json.as_h.has_key?("Bindings").should be_true
      end
    end

    it "should trigger garbage collection and print stats" do
      with_http_server do |(http, s)|
        before = GC.prof_stats.gc_no
        result = run_lavinmqctl(http.addr.to_s, ["gc_collect"])
        result[:exit].should eq(0)
        GC.prof_stats.gc_no.should be > before
        result[:stdout].should contain("gc_no")
      end
    end

    it "should create and delete queue" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["create_queue", "new_queue"])
        result[:exit].should eq(0)

        vhost = s.vhosts["/"]
        vhost.queue_exists?("new_queue").should be_true

        result = run_lavinmqctl(http.addr.to_s, ["delete_queue", "new_queue"])
        result[:exit].should eq(0)
      end
    end

    it "should add and delete user" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["add_user", "newuser", "password123"])
        result[:exit].should eq(0)

        s.users["newuser"]?.should_not be_nil

        result = run_lavinmqctl(http.addr.to_s, ["delete_user", "newuser"])
        result[:exit].should eq(0)
      end
    end

    it "should handle connection errors gracefully" do
      result = run_lavinmqctl("localhost:99999", ["list_users"])
      result[:exit].should eq(1)
    end

    it "should list exchanges" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_exchanges"])
        result[:exit].should eq(0)
        result[:stdout].should contain("amq.direct")
      end
    end

    it "should change user password" do
      with_http_server do |(http, s)|
        s.users.create("testuser", "oldpass", [LavinMQ::Tag::Management])
        result = run_lavinmqctl(http.addr.to_s, ["change_password", "testuser", "newpass"])
        result[:exit].should eq(0)
      end
    end

    it "should set user tags" do
      with_http_server do |(http, s)|
        s.users.create("testuser", "password", [] of LavinMQ::Tag)
        result = run_lavinmqctl(http.addr.to_s, ["set_user_tags", "testuser", "administrator"])
        result[:exit].should eq(0)
      end
    end

    it "should add and delete vhost" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["add_vhost", "test_vhost"])
        result[:exit].should eq(0)

        s.vhosts.has_key?("test_vhost").should be_true

        result = run_lavinmqctl(http.addr.to_s, ["delete_vhost", "test_vhost"])
        result[:exit].should eq(0)
      end
    end

    it "should purge queue" do
      with_http_server do |(http, s)|
        vhost = s.vhosts["/"]
        vhost.declare_queue("test_queue", true, false)

        result = run_lavinmqctl(http.addr.to_s, ["purge_queue", "test_queue"])
        result[:exit].should eq(0)
      end
    end

    it "should pause and resume queue" do
      with_http_server do |(http, s)|
        vhost = s.vhosts["/"]
        vhost.declare_queue("test_queue", true, false)

        result = run_lavinmqctl(http.addr.to_s, ["pause_queue", "test_queue"])
        result[:exit].should eq(0)

        result = run_lavinmqctl(http.addr.to_s, ["resume_queue", "test_queue"])
        result[:exit].should eq(0)
      end
    end

    it "should create and delete exchange" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["create_exchange", "topic", "test_exchange"])
        result[:exit].should eq(0)

        vhost = s.vhosts["/"]
        vhost.exchange_exists?("test_exchange").should be_true

        result = run_lavinmqctl(http.addr.to_s, ["delete_exchange", "test_exchange"])
        result[:exit].should eq(0)
      end
    end

    it "should create exchange with durable flag" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["create_exchange", "--durable", "direct", "test_durable_exchange"])
        result[:exit].should eq(0)

        vhost = s.vhosts["/"]
        vhost.exchange_exists?("test_durable_exchange").should be_true
      end
    end

    it "should create durable queue" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["create_queue", "--durable", "test_durable_queue"])
        result[:exit].should eq(0)

        vhost = s.vhosts["/"]
        vhost.queue_exists?("test_durable_queue").should be_true
      end
    end

    it "should set and clear policy" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["set_policy", "test_policy", ".*", "{\"max-length\":100}"])
        result[:exit].should eq(0)

        result = run_lavinmqctl(http.addr.to_s, ["list_policies"])
        result[:exit].should eq(0)
        result[:stdout].should contain("test_policy")

        result = run_lavinmqctl(http.addr.to_s, ["clear_policy", "test_policy"])
        result[:exit].should eq(0)
      end
    end

    it "should list policies for vhost" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_policies"])
        result[:exit].should eq(0)
      end
    end

    it "should list connections" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_connections"])
        result[:exit].should eq(0)
      end
    end

    it "should list connections in JSON format" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_connections", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_a?.should_not be_nil
      end
    end

    it "should export definitions" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["export_definitions"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_h?.should_not be_nil
        json.as_h.has_key?("users").should be_true
        json.as_h.has_key?("vhosts").should be_true
      end
    end

    it "should set permissions" do
      with_http_server do |(http, s)|
        s.users.create("testuser", "password", [LavinMQ::Tag::Management])
        result = run_lavinmqctl(http.addr.to_s, ["set_permissions", "testuser", ".*", ".*", ".*"])
        result[:exit].should eq(0)
      end
    end

    it "should set vhost limits" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["set_vhost_limits", "{\"max-connections\":100}"])
        result[:exit].should eq(0)
      end
    end

    it "should hash password" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["hash_password", "testpassword"])
        result[:exit].should eq(0)
        result[:stdout].should_not be_empty
      end
    end

    it "should show cluster status" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["cluster_status"])
        result[:exit].should eq(0)
      end
    end

    it "should start TUI with parsed interval" do
      called = false
      interval = nil.as(Float64?)
      LavinMQCtl.tui_launcher = ->(_client : HTTP::Client, parsed_interval : Float64) {
        called = true
        interval = parsed_interval
      }

      result = run_lavinmqctl("localhost:15672", ["tui", "-i", "2.5"])
      result[:exit].should eq(0)
      called.should be_true
      interval.should eq(2.5)
    ensure
      LavinMQCtl.tui_launcher = nil
    end

    # Error cases
    it "should fail when TUI interval is invalid" do
      called = false
      LavinMQCtl.tui_launcher = ->(_client : HTTP::Client, _interval : Float64) {
        called = true
      }

      result = run_lavinmqctl("localhost:15672", ["tui", "-i", "0"])
      result[:exit].should eq(1)
      result[:stderr].should contain("Invalid interval: 0")
      called.should be_false
    ensure
      LavinMQCtl.tui_launcher = nil
    end

    it "should fail when creating user with missing password" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["add_user", "testuser"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when deleting non-existent user" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["delete_user", "nonexistent"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when deleting non-existent queue" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["delete_queue", "nonexistent_queue"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when purging non-existent queue" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["purge_queue", "nonexistent_queue"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when setting policy with invalid JSON" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["set_policy", "test_policy", ".*", "invalid-json"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when creating exchange without type" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["create_exchange"])
        result[:exit].should eq(1)
      end
    end

    it "should fail when setting vhost limits with invalid JSON" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["set_vhost_limits", "not-json"])
        result[:exit].should eq(1)
      end
    end

    it "should list exchanges in JSON format" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_exchanges", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_a?.should_not be_nil
      end
    end

    it "should list vhosts in JSON format" do
      with_http_server do |(http, s)|
        result = run_lavinmqctl(http.addr.to_s, ["list_vhosts", "--format=json"])
        result[:exit].should eq(0)
        json = JSON.parse(result[:stdout])
        json.as_a?.should_not be_nil
      end
    end
  end
end
