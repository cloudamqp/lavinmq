require "./lavinmqctl/cli"

cli = LavinMQCtl.new
begin
  cli.run_cmd
rescue ex : LavinMQCtl::CtlExit
  STDERR.puts ex.message unless ex.message.to_s.empty?
  exit ex.code
end
