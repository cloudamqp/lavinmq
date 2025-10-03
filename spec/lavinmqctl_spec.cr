require "./spec_helper"

# Build lavinmqctl binary before running tests
unless File.exists?("./bin/lavinmqctl")
  puts "Building lavinmqctl..."
  status = Process.run("make", ["bin/lavinmqctl"], output: STDOUT, error: STDERR)
  abort "Failed to build lavinmqctl" unless status.success?
end

# Helper to run lavinmqctl commands against test server
def run_lavinmqctl(http_addr : String, argv : Array(String))
  stdout = IO::Memory.new
  stderr = IO::Memory.new

  args = ["--uri", "http://#{http_addr}", "--user", "guest", "--password", "guest"] + argv

  status = Process.run(
    "./bin/lavinmqctl",
    args: args,
    output: stdout,
    error: stderr
  )

  {
    stdout: stdout.to_s,
    stderr: stderr.to_s,
    exit:   status.exit_code,
  }
rescue ex
  {
    stdout: "",
    stderr: ex.message.to_s,
    exit:   1,
  }
end

describe "LavinMQCtl" do
  describe "list_users" do
    it "should list users in text format" do
      with_http_server do |http, s|
        s.users.create("testuser", "testpass", [LavinMQ::Tag::Management])
        result = run_lavinmqctl(http.addr, ["list_users"])
        result[:stdout].should contain("guest")
        result[:stdout].should contain("testuser")
        result[:stdout].should contain("name\ttags")
      end
    end

    it "should list users in JSON format" do
      with_http_server do |http, s|
        s.users.create("testuser", "testpass", [LavinMQ::Tag::Management])
        result = run_lavinmqctl(http.addr, ["list_users", "-f", "json"])
        data = JSON.parse(result[:stdout])
        data.as_a.size.should be >= 2
        names = data.as_a.map(&.["name"].as_s)
        names.should contain("guest")
        names.should contain("testuser")
      end
    end

    it "should suppress informational messages with -q flag" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["list_users", "-q"])
        result[:stdout].should_not contain("Listing users")
      end
    end
  end

  describe "add_user" do
    it "should create a new user" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["add_user", "newuser", "newpass"])
        result[:exit].should eq(0)
        s.users["newuser"]?.should_not be_nil
        s.users["newuser"].password.try(&.verify("newpass")).should be_true
      end
    end

    it "should fail without required arguments" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["add_user"])
        result[:exit].should eq(1)
      end
    end
  end

  describe "delete_user" do
    it "should delete a user" do
      with_http_server do |http, s|
        s.users.create("todelete", "pass")
        result = run_lavinmqctl(http.addr, ["delete_user", "todelete"])
        result[:exit].should eq(0)
        s.users["todelete"]?.should be_nil
      end
    end

    it "should handle non-existent user" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["delete_user", "nonexistent"])
        result[:exit].should eq(1)
      end
    end
  end

  describe "list_queues" do
    it "should list queues in text format" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("testqueue", true, false)
        result = run_lavinmqctl(http.addr, ["list_queues"])
        result[:stdout].should contain("testqueue")
        result[:stdout].should contain("name\tmessages")
      end
    end

    it "should list queues in JSON format" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("testqueue", true, false)
        result = run_lavinmqctl(http.addr, ["list_queues", "-f", "json"])
        data = JSON.parse(result[:stdout])
        data.as_a.map(&.["name"].as_s).should contain("testqueue")
      end
    end

    it "should list queues for specific vhost" do
      with_http_server do |http, s|
        s.vhosts.create("testvhost")
        s.vhosts["testvhost"].declare_queue("vhostqueue", true, false)
        result = run_lavinmqctl(http.addr, ["list_queues", "-p", "testvhost"])
        result[:stdout].should contain("vhostqueue")
      end
    end
  end

  describe "create_queue" do
    it "should create a queue" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["create_queue", "newqueue"])
        result[:exit].should eq(0)
        s.vhosts["/"].queues["newqueue"]?.should_not be_nil
      end
    end

    it "should create a durable queue" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["create_queue", "durablequeue", "--durable"])
        result[:exit].should eq(0)
        s.vhosts["/"].queues["durablequeue"].durable?.should be_true
      end
    end
  end

  describe "status" do
    it "should display server status in text format" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["status"])
        result[:stdout].should contain("Version:")
        result[:stdout].should contain("Connections:")
        result[:stdout].should contain("Queues:")
      end
    end

    it "should display server status in JSON format" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["status", "-f", "json"])
        data = JSON.parse(result[:stdout])
        data["Version"]?.should_not be_nil
        data["Connections"]?.should_not be_nil
        data["Queues"]?.should_not be_nil
      end
    end
  end

  describe "list_vhosts" do
    it "should list vhosts" do
      with_http_server do |http, s|
        s.vhosts.create("test1")
        s.vhosts.create("test2")
        result = run_lavinmqctl(http.addr, ["list_vhosts"])
        result[:stdout].should contain("/")
        result[:stdout].should contain("test1")
        result[:stdout].should contain("test2")
      end
    end

    it "should list vhosts in JSON format" do
      with_http_server do |http, s|
        s.vhosts.create("jsonvhost")
        result = run_lavinmqctl(http.addr, ["list_vhosts", "-f", "json"])
        data = JSON.parse(result[:stdout])
        names = data.as_a.map(&.["name"].as_s)
        names.should contain("/")
        names.should contain("jsonvhost")
      end
    end
  end

  describe "add_vhost" do
    it "should create a vhost" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["add_vhost", "newvhost"])
        result[:exit].should eq(0)
        s.vhosts["newvhost"]?.should_not be_nil
      end
    end
  end

  describe "delete_vhost" do
    it "should delete a vhost" do
      with_http_server do |http, s|
        s.vhosts.create("todelete")
        result = run_lavinmqctl(http.addr, ["delete_vhost", "todelete"])
        result[:exit].should eq(0)
        s.vhosts["todelete"]?.should be_nil
      end
    end
  end

  describe "export_definitions" do
    it "should export definitions" do
      with_http_server do |http, s|
        s.users.create("exportuser", "pass")
        s.vhosts.create("exportvhost")
        result = run_lavinmqctl(http.addr, ["export_definitions"])
        result[:exit].should eq(0)
        data = JSON.parse(result[:stdout])
        data["users"]?.should_not be_nil
        data["vhosts"]?.should_not be_nil
      end
    end

    it "should export definitions for specific vhost" do
      with_http_server do |http, s|
        s.vhosts.create("specific")
        s.vhosts["specific"].declare_queue("q1", true, false)
        result = run_lavinmqctl(http.addr, ["export_definitions", "-p", "specific"])
        result[:exit].should eq(0)
        data = JSON.parse(result[:stdout])
        data["queues"]?.should_not be_nil
      end
    end
  end

  describe "set_user_tags" do
    it "should set user tags" do
      with_http_server do |http, s|
        s.users.create("taguser", "pass")
        result = run_lavinmqctl(http.addr, ["set_user_tags", "taguser", "management", "monitoring"])
        result[:exit].should eq(0)
        s.users["taguser"].tags.should contain(LavinMQ::Tag::Management)
        s.users["taguser"].tags.should contain(LavinMQ::Tag::Monitoring)
      end
    end
  end

  describe "hash_password" do
    it "should hash a password" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["hash_password", "testpass"])
        result[:exit].should eq(0)
        result[:stdout].should_not be_empty
        result[:stdout].size.should be > 40
      end
    end
  end

  describe "error handling" do
    it "should handle 404 errors" do
      with_http_server do |http, _|
        result = run_lavinmqctl(http.addr, ["delete_queue", "nonexistent"])
        result[:exit].should eq(1)
      end
    end

    it "should handle connection errors gracefully" do
      # Test with invalid hostname
      result = run_lavinmqctl("localhost:99999", ["list_users"])
      result[:exit].should eq(1)
    end
  end

  describe "list_exchanges" do
    it "should list exchanges" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("testexchange", "direct", true, false)
        result = run_lavinmqctl(http.addr, ["list_exchanges"])
        result[:stdout].should contain("testexchange")
        result[:stdout].should contain("direct")
      end
    end

    it "should list exchanges in JSON format" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("jsonexchange", "topic", true, false)
        result = run_lavinmqctl(http.addr, ["list_exchanges", "-f", "json"])
        data = JSON.parse(result[:stdout])
        exchanges = data.as_a.map { |e| {e["name"].as_s, e["type"].as_s} }
        exchanges.should contain({"jsonexchange", "topic"})
      end
    end
  end

  describe "create_exchange" do
    it "should create an exchange" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["create_exchange", "direct", "newexchange"])
        result[:exit].should eq(0)
        s.vhosts["/"].exchanges["newexchange"]?.should_not be_nil
      end
    end

    it "should create a durable exchange" do
      with_http_server do |http, s|
        result = run_lavinmqctl(http.addr, ["create_exchange", "topic", "durableex", "--durable"])
        result[:exit].should eq(0)
        s.vhosts["/"].exchanges["durableex"].durable?.should be_true
      end
    end
  end
end
