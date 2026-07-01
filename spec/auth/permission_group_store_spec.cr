require "../spec_helper"

describe LavinMQ::Auth::PermissionGroupStore do
  it "persists and reloads groups" do
    dir = File.tempname
    Dir.mkdir_p dir
    begin
      store = LavinMQ::Auth::PermissionGroupStore.new(dir, nil)
      rule = LavinMQ::Auth::PermissionGroup::Rule.new("chat/{client_id}/#", read: true, write: true)
      store.put(LavinMQ::Auth::PermissionGroup.new("chat", "mqtt", false, ["alice"], [rule]))

      reloaded = LavinMQ::Auth::PermissionGroupStore.new(dir, nil)
      reloaded["chat"]?.should_not be_nil
      reloaded["chat"].members.should eq ["alice"]
      reloaded["chat"].rules.first.pattern.should eq "chat/{client_id}/#"
    ensure
      FileUtils.rm_rf dir
    end
  end

  it "bumps revision on mutation" do
    dir = File.tempname
    Dir.mkdir_p dir
    begin
      store = LavinMQ::Auth::PermissionGroupStore.new(dir, nil)
      before = store.revision
      store.put(LavinMQ::Auth::PermissionGroup.new("g", "mqtt"))
      store.revision.should_not eq before
    ensure
      FileUtils.rm_rf dir
    end
  end

  it "resolves mqtt groups for a user including apply_to_all" do
    dir = File.tempname
    Dir.mkdir_p dir
    begin
      store = LavinMQ::Auth::PermissionGroupStore.new(dir, nil)
      store.put(LavinMQ::Auth::PermissionGroup.new("a", "mqtt", false, ["alice"]))
      store.put(LavinMQ::Auth::PermissionGroup.new("all", "mqtt", true))
      store.put(LavinMQ::Auth::PermissionGroup.new("b", "mqtt", false, ["bob"]))
      names = store.for_mqtt_user("alice").map(&.name).sort!
      names.should eq ["a", "all"]
    ensure
      FileUtils.rm_rf dir
    end
  end
end
