require "./spec_helper"
require "file_utils"

describe LavinMQ::VHost do
  it "should handle definitions files from v2.4.3 with frame size errors" do
    Dir.mkdir_p File.join(LavinMQ::Config.instance.data_dir, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")
    File.open(File.join(LavinMQ::Config.instance.data_dir, "vhosts.json"), "w") do |f|
      [{"name": "test", "dir": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"}].to_json(f)
    end
    FileUtils.cp(
      File.join(__DIR__, "fixtures", "v243_e2e_binding.amqp"),
      File.join(
        LavinMQ::Config.instance.data_dir,
        "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        "definitions.amqp"
      )
    )
    with_amqp_server do |s|
      exchanges = s.vhosts["test"].exchanges
      src_x = exchanges["source.exchange"]
      dst_x = exchanges["destination.exchange"]
      src_x.bindings_details.to_a.should_not be_empty
      dst_x.bindings_details.to_a.should_not be_empty
    end
  end
end
