require "./spec_helper"
require "../src/lavinmq/definitions_generator"

private def with_data_dir(&)
  data_dir = File.tempname
  Dir.mkdir_p data_dir
  File.write(File.join(data_dir, "vhosts.json"), "[]")
  File.write(File.join(data_dir, "users.json"), "[]")
  yield data_dir
ensure
  FileUtils.rm_rf data_dir if data_dir
end

describe LavinMQCtl::DefinitionsGenerator do
  it "includes permission groups read from permission_groups.json" do
    with_data_dir do |data_dir|
      groups = [{name: "g1", protocol: "mqtt", members: ["*"],
                 rules: [{pattern: "a/#", read: true, write: false}]}]
      File.write(File.join(data_dir, "permission_groups.json"), groups.to_json)

      io = IO::Memory.new
      LavinMQCtl::DefinitionsGenerator.new(data_dir).generate(io)
      body = JSON.parse(io.to_s)

      body["permission_groups"].as_a.size.should eq 1
      group = body["permission_groups"][0]
      group["name"].as_s.should eq "g1"
      group["rules"][0]["pattern"].as_s.should eq "a/#"
    end
  end

  it "omits the permission_groups key entirely when the file is absent" do
    with_data_dir do |data_dir|
      io = IO::Memory.new
      LavinMQCtl::DefinitionsGenerator.new(data_dir).generate(io)
      body = JSON.parse(io.to_s)

      body.as_h.has_key?("permission_groups").should be_false
    end
  end
end
