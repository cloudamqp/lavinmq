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

def run_lavinmqctl_offline(argv : Array(String))
  stdout_capture = IO::Memory.new
  stderr_capture = IO::Memory.new
  stderr = ""
  exit_code = 0

  original_argv = ARGV.dup
  begin
    ARGV.clear
    ARGV.concat(argv)

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
  describe "with offline data dir" do
    it "exports bindings from compact snapshots with omitted default fields" do
      with_datadir do |data_dir|
        vhost_dir = File.join(data_dir, "vhost")
        Dir.mkdir_p vhost_dir
        File.write(File.join(data_dir, "vhosts.json"), %([{"name":"test","dir":"vhost"}]))
        File.write(File.join(data_dir, "users.json"),
          %([{"name":"guest","password_hash":"hash","hashing_algorithm":"SHA256","tags":"administrator","permissions":{"test":{"config":".*","read":".*","write":".*"}}}]))
        File.write(File.join(vhost_dir, "exchanges.json"),
          %([{"name":"direct","type":"direct"},{"name":"fanout","type":"fanout"}]))
        File.write(File.join(vhost_dir, "queues.json"),
          %([{"name":"queue_rk"},{"name":"queue_empty"},{"name":"queue_wal"}]))
        File.write(File.join(vhost_dir, "bindings.json"),
          %([{"source":"direct","destination":"queue_rk","routing_key":"rk"},{"source":"fanout","destination":"queue_empty"}]))
        File.write(File.join(vhost_dir, "definitions.jsonl"),
          %({"op":"queue.bind","queue":"queue_wal","exchange":"fanout","routing_key":""}\n))

        snapshot_bindings = JSON.parse(File.read(File.join(vhost_dir, "bindings.json"))).as_a
        queue_binding = snapshot_bindings.find! { |b| b["destination"].as_s == "queue_rk" }
        queue_binding.as_h.has_key?("destination_type").should be_false
        empty_routing_key_binding = snapshot_bindings.find! { |b| b["destination"].as_s == "queue_empty" }
        empty_routing_key_binding.as_h.has_key?("destination_type").should be_false
        empty_routing_key_binding.as_h.has_key?("routing_key").should be_false

        result = run_lavinmqctl_offline(["definitions", data_dir])
        result[:exit].should eq(0)

        bindings = JSON.parse(result[:stdout])["bindings"].as_a
        exported_queue_binding = bindings.find! { |b| b["destination"].as_s == "queue_rk" }
        exported_queue_binding["destination_type"].as_s.should eq "queue"
        exported_queue_binding["routing_key"].as_s.should eq "rk"

        exported_empty_routing_key_binding = bindings.find! { |b| b["destination"].as_s == "queue_empty" }
        exported_empty_routing_key_binding["destination_type"].as_s.should eq "queue"
        exported_empty_routing_key_binding["routing_key"].as_s.should eq ""

        exported_wal_binding = bindings.find! { |b| b["destination"].as_s == "queue_wal" }
        exported_wal_binding["destination_type"].as_s.should eq "queue"
        exported_wal_binding["routing_key"].as_s.should eq ""
      end
    end
  end

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

    # Error cases
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
