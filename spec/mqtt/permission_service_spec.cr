require "../spec_helper"
require "../../src/lavinmq/mqtt/permission_service"

private def group(name, members, rules)
  LavinMQ::MQTT::PermissionGroup.new(name, "/", members, rules)
end

private def rule(pattern, read = false, write = false)
  identifier = pattern.gsub(/[^A-Za-z0-9-]/, "-")
  LavinMQ::MQTT::PermissionGroup::Rule.new(identifier, pattern, read: read, write: write)
end

private def ctx(username, client_id = "dev")
  LavinMQ::MQTT::PermissionService::Context.new(username, client_id)
end

private def with_data_dir(&)
  data_dir = File.tempname
  Dir.mkdir_p data_dir
  begin
    yield data_dir
  ensure
    FileUtils.rm_rf data_dir
  end
end

private def lock_down(service)
  service.delete(LavinMQ::MQTT::PermissionService::DEFAULT_GROUP)
  service
end

# A fresh vhost is open through its default group. Most examples test what a
# rule grants, so they start from a locked-down service.
private def with_service(&)
  with_data_dir do |data_dir|
    yield lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
  end
end

# commit writes to the follower sockets while it still holds @save_lock, and
# that write can suspend the fiber. Two writers then park in Mutex#lock at the
# same time, each holding a group it read before it parked. This stub makes
# that suspension deterministic.
class ParkingReplicator
  include LavinMQ::Clustering::Replicator

  property? armed = false

  def initialize(&@park : -> Nil)
  end

  def replace_file(path : String)
    return unless @armed
    @armed = false
    @park.call
  end

  def register_file(path : String)
  end

  def register_file(file : File)
  end

  def register_file(mfile : MFile)
  end

  def replace_file(mfile : MFile)
  end

  def append(path : String, pos : Int, length : Int)
  end

  def append_value(path : String, value : UInt32 | Int32, offset : Int64)
  end

  def append_bytes(path : String, bytes : Bytes, offset : Int64)
  end

  def delete_file(path : String)
  end

  def followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def syncing_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def all_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def isr_dirty? : Bool
    false
  end

  def flush_isr : Nil
  end

  def wait_for_followers : Nil
  end

  def close
  end

  def listen(server : TCPServer)
  end

  def clear
  end

  def password : String
    ""
  end
end

describe LavinMQ::MQTT::PermissionService do
  it "seeds a default group that allows every user every topic" do
    with_data_dir do |data_dir|
      service = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      default = service[LavinMQ::MQTT::PermissionService::DEFAULT_GROUP]?.should_not be_nil
      default.members.should eq ["*"]
      service.can_write?(ctx("c1"), "a/b").should be_true
      service.can_read?(ctx(nil), "a/b").should be_true
    end
  end

  it "keeps the default group's grant next to a narrower group" do
    with_data_dir do |data_dir|
      service = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.can_write?(ctx("c2"), "b/c").should be_true
    end
  end

  it "persists the groups on the first change" do
    with_data_dir do |data_dir|
      service = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      path = File.join(data_dir, "mqtt_permissions.json")
      File.exists?(path).should be_false
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      File.exists?(path).should be_true
      reloaded = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      reloaded["g"]?.should_not be_nil
      reloaded[LavinMQ::MQTT::PermissionService::DEFAULT_GROUP]?.should_not be_nil
    end
    with_data_dir do |data_dir|
      service = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      path = File.join(data_dir, "mqtt_permissions.json")
      service.delete("nonexistent")
      File.exists?(path).should be_false
      service.delete(LavinMQ::MQTT::PermissionService::DEFAULT_GROUP)
      JSON.parse(File.read(path)).as_a.should be_empty
    end
  end

  it "denies everything once the default group is deleted" do
    with_service do |service|
      service.can_write?(ctx("c1"), "a/b").should be_false
      service.can_read?(ctx("c1"), "a/b").should be_false
    end
  end

  it "does not activate a new grant when saving fails" do
    with_data_dir do |data_dir|
      service = lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
      path = File.join(data_dir, "mqtt_permissions.json")
      original = File.read(path)
      Dir.mkdir("#{path}.tmp")
      expect_raises(LavinMQ::MQTT::PermissionService::SaveError) do
        service.put(group("g", ["c1"], [rule("a/#", read: true, write: true)]))
      end
      service["g"]?.should be_nil
      service.can_read?(ctx("c1"), "a/b").should be_false
      service.can_write?(ctx("c1"), "a/b").should be_false
      File.read(path).should eq original
    end
  end

  it "keeps a deleted group's grants when saving the deletion fails" do
    with_data_dir do |data_dir|
      service = lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
      service.put(group("g", ["c1"], [rule("a/#", read: true, write: true)]))
      path = File.join(data_dir, "mqtt_permissions.json")
      original = File.read(path)
      Dir.mkdir("#{path}.tmp")
      expect_raises(LavinMQ::MQTT::PermissionService::SaveError) { service.delete("g") }
      service["g"]?.should_not be_nil
      service.can_read?(ctx("c1"), "a/b").should be_true
      service.can_write?(ctx("c1"), "a/b").should be_true
      File.read(path).should eq original
    end
  end

  it "keeps concurrent group additions in memory and on disk" do
    with_data_dir do |data_dir|
      service = lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
      done = Channel(Exception?).new(10)
      10.times do |i|
        spawn do
          service.put(group("g#{i}", ["c1"], [rule("a#{i}/#", read: true)]))
          done.send(nil)
        rescue ex
          done.send(ex)
        end
      end
      10.times { done.receive.should be_nil }
      reloaded = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      [service, reloaded].each do |permissions|
        permissions.size.should eq 10
        10.times { |i| permissions.can_read?(ctx("c1"), "a#{i}/b").should be_true }
      end
    end
  end

  it "keeps the automatic default group in memory only" do
    with_data_dir do |data_dir|
      LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      File.exists?(File.join(data_dir, "mqtt_permissions.json")).should be_false
      # The next start creates it again, so an unchanged vhost stays open.
      again = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      again.can_write?(ctx("c1"), "a/b").should be_true
    end
  end

  it "does not create the default group again after it was deleted" do
    with_data_dir do |data_dir|
      lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
      reloaded = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      reloaded.size.should eq 0
      reloaded.can_write?(ctx("c1"), "a/b").should be_false
    end
  end

  it "rejects an invalid group on put" do
    with_service do |service|
      expect_raises(ArgumentError, /Invalid MQTT topic filter/) do
        service.put(group("g", ["*"], [rule("a/#/b", write: true)]))
      end
      service.can_write?(ctx("c1"), "a/b").should be_false
    end
  end

  # The block runs under the same lock as the commit, so it always sees the
  # group as it is at commit time.
  it "updates a group from its state at commit time" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      changed = service.update("g") do |current|
        LavinMQ::MQTT::PermissionGroup.new(current.name, current.vhost, current.members,
          current.rules + [rule("b/#", read: true)])
      end
      changed.should be_true
      service.can_read?(ctx("c1"), "a/x").should be_true
      service.can_read?(ctx("c1"), "b/x").should be_true
    end
  end

  it "creates a group only when the name is free" do
    with_service do |service|
      service.create(group("g", ["c1"], [rule("a/#", read: true)])).should be_true
      service.create(group("g", ["c1"], [rule("b/#", read: true)])).should be_false
      service.can_read?(ctx("c1"), "a/x").should be_true
      service.can_read?(ctx("c1"), "b/x").should be_false
    end
  end

  # Regression: a read outside the lock goes stale while the fiber parks in
  # Mutex#lock, even though the read itself was fresh.
  it "does not lose a change made by another writer parked on the lock" do
    with_data_dir do |data_dir|
      release = Channel(Nil).new
      replicator = ParkingReplicator.new { release.receive }
      service = lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, replicator))
      service.put(group("g", ["c1"], [rule("keep/#", read: true)]))
      replicator.armed = true # only the first writer parks inside commit

      done = Channel(Exception?).new(3)
      spawn(name: "holder") do
        service.update("g") do |current|
          LavinMQ::MQTT::PermissionGroup.new(current.name, current.vhost, current.members,
            current.rules + [rule("first/#", read: true)])
        end
        done.send(nil)
      rescue ex
        done.send(ex)
      end
      Fiber.yield # the holder is now parked inside commit, holding @save_lock

      spawn(name: "member") do
        service.update("g") do |current|
          LavinMQ::MQTT::PermissionGroup.new(current.name, current.vhost, current.members + ["c2"],
            current.rules)
        end
        done.send(nil)
      rescue ex
        done.send(ex)
      end
      Fiber.yield # parked in Mutex#lock behind the holder

      spawn(name: "rule") do
        service.update("g") do |current|
          LavinMQ::MQTT::PermissionGroup.new(current.name, current.vhost, current.members,
            current.rules + [rule("second/#", read: true)])
        end
        done.send(nil)
      rescue ex
        done.send(ex)
      end
      Fiber.yield # also parked in Mutex#lock

      release.send(nil)
      3.times { done.receive.should be_nil }

      service.can_read?(ctx("c2"), "keep/x").should be_true # the member survived
      service.can_read?(ctx("c1"), "first/x").should be_true
      service.can_read?(ctx("c1"), "second/x").should be_true
    end
  end

  it "reports a missing group on update" do
    with_service do |service|
      service.update("nope") { |current| current }.should be_false
      service.size.should eq 0
    end
  end

  it "keeps the group as it is when the update block returns nil" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.update("g") { nil }.should be_true
      service.can_read?(ctx("c1"), "a/x").should be_true
    end
  end

  it "rejects an invalid group on update" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      expect_raises(ArgumentError, /Invalid MQTT topic filter/) do
        service.update("g") do |current|
          LavinMQ::MQTT::PermissionGroup.new(current.name, current.vhost, current.members,
            [rule("a/#/b", read: true)])
        end
      end
      service.can_read?(ctx("c1"), "a/x").should be_true
    end
  end

  it "grants only the requested verb" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.can_read?(ctx("c1"), "a/b").should be_true
      service.can_write?(ctx("c1"), "a/b").should be_false
    end
  end

  it "applies a group only to its members" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", write: true)]))
      service.can_write?(ctx("c1"), "a/b").should be_true
      service.can_write?(ctx("c2"), "a/b").should be_false
    end
  end

  it "never attributes one group's rules to another group's members" do
    with_service do |service|
      service.put(group("a", ["c1"], [rule("a/#", read: true)]))
      service.put(group("b", ["c2"], [rule("b/#", write: true)]))

      service.can_read?(ctx("c1"), "a/x").should be_true
      service.can_read?(ctx("c2"), "a/x").should be_false
      service.can_write?(ctx("c1"), "b/x").should be_false
      service.can_write?(ctx("c2"), "b/x").should be_true

      service.can_write?(ctx("c1"), "a/x").should be_false
      service.can_read?(ctx("c2"), "b/x").should be_false
    end
  end

  it "applies a wildcard-member group to every client" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("a/#", write: true)]))
      service.can_write?(ctx("anyone", "anything"), "a/b").should be_true
    end
  end

  it "binds {client_id} to the requesting client" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("data/{client_id}/#", read: true, write: true)]))
      service.can_write?(ctx("u", "c1"), "data/c1/temp").should be_true
      service.can_write?(ctx("u", "c1"), "data/c2/temp").should be_false
    end
  end

  it "reflects an update on the next check" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("a/#", write: true)]))
      service.can_write?(ctx("c1"), "a/b").should be_true

      service.put(group("g", ["*"], [rule("a/#", read: true)]))
      service.can_write?(ctx("c1"), "a/b").should be_false
      service.can_read?(ctx("c1"), "a/b").should be_true
    end
  end

  it "reflects a delete on the next check" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.can_read?(ctx("c1"), "a/b").should be_true
      service.delete("g")
      service.can_read?(ctx("c1"), "a/b").should be_false
    end
  end

  it "fails closed on {client_id} matching when the client id is the '#' wildcard" do
    with_service do |service|
      service.put(group("g", ["*"], [
        rule("data/{client_id}/#", read: true),
        rule("static/topic", read: true),
      ]))
      service.can_read?(ctx("u", "#"), "data/anything/temp").should be_false
      service.can_read?(ctx("u", "#"), "static/topic").should be_true
    end
  end

  it "survives a reload from disk" do
    with_data_dir do |data_dir|
      first = lock_down(LavinMQ::MQTT::PermissionService.new("/", data_dir, nil))
      first.put(group("g", ["c1"], [rule("a/#", write: true)]))

      second = LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
      second.can_write?(ctx("c1"), "a/b").should be_true
      second.can_write?(ctx("c2"), "a/b").should be_false
    end
  end

  # Everything put accepts is valid, so an invalid group on disk is a hand
  # edit or a foreign writer. Loading it would make the group unmodifiable
  # through the API, since every put revalidates the whole group.
  {
    {"an invalid pattern", /Invalid MQTT topic filter/,
     %([{"name":"g","vhost":"/","members":["c1"],"rules":[{"identifier":"bad","pattern":"bad/#/x","write":true}]}])},
    {"an invalid name", /Invalid group name/,
     %([{"name":"my group","vhost":"/","members":["c1"],"rules":[{"identifier":"ok","pattern":"ok/#","write":true}]}])},
    {"duplicate rule identifiers", /Duplicate rule identifier/,
     %([{"name":"g","vhost":"/","members":["c1"],"rules":[{"identifier":"r","pattern":"a/#","write":true},{"identifier":"r","pattern":"b/#","write":true}]}])},
  }.each do |what, message, json|
    it "refuses to load a group with #{what} from disk" do
      data_dir = File.tempname
      Dir.mkdir_p data_dir
      begin
        File.write File.join(data_dir, "mqtt_permissions.json"), json
        expect_raises(ArgumentError, message) do
          LavinMQ::MQTT::PermissionService.new("/", data_dir, nil)
        end
      ensure
        FileUtils.rm_rf data_dir
      end
    end
  end

  it "grants nothing for a group with rules but no members" do
    with_service do |service|
      service.put(group("g", Array(String).new, [rule("a/#", read: true)]))
      service.can_read?(ctx("c1"), "a/b").should be_false
    end
  end

  it "grants nothing for a group with an empty rules array" do
    with_service do |service|
      service.put(group("g", ["*"], Array(LavinMQ::MQTT::PermissionGroup::Rule).new))
      service.can_read?(ctx("c1"), "a/b").should be_false
      service.put(group("g", ["*"], [rule("a/#", read: true)]))
      service.can_read?(ctx("c1"), "a/b").should be_true
    end
  end

  it "gives a client the union of rules from every group it is a member of" do
    with_service do |service|
      service.put(group("a", ["c1"], [rule("a/#", read: true)]))
      service.put(group("b", ["c1"], [rule("b/#", write: true)]))
      service.can_read?(ctx("c1"), "a/x").should be_true
      service.can_write?(ctx("c1"), "b/x").should be_true
      service.can_write?(ctx("c1"), "a/x").should be_false
      service.can_read?(ctx("c1"), "b/x").should be_false
    end
  end

  it "applies only wildcard-member rules when the username is unknown" do
    with_service do |service|
      service.put(group("a", ["*"], [rule("a/#", read: true)]))
      service.put(group("b", ["c1"], [rule("b/#", read: true)]))
      service.can_read?(ctx(nil), "a/x").should be_true
      service.can_read?(ctx(nil), "b/x").should be_false
    end
  end

  it "keeps a shared group's rules intact when one member is also in another group" do
    with_service do |service|
      service.put(group("a", ["c1", "c2"], [rule("a/#", read: true)]))
      service.put(group("b", ["c1"], [rule("b/#", write: true)]))
      service.can_write?(ctx("c1"), "b/x").should be_true
      service.can_write?(ctx("c2"), "b/x").should be_false
      service.can_read?(ctx("c2"), "a/x").should be_true
    end
  end
end
