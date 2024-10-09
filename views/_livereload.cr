require "live_reload"
require "inotify"

dir = "#{__DIR__}/../static/views"
live_reload = LiveReload::Server.new
watcher = Inotify.watch dir do |event|
  puts "File changed: #{event.inspect}"
  path = event.path
  next unless path
  sleep 2 # wait until the whole file is written
  live_reload.send_reload(path: path, liveCSS: path.ends_with?(".css"))
end

puts "Watching changes from #{dir}"
puts "LiveReload on http://#{live_reload.address}"

Process.on_terminate do |_reason|
  watcher.close
  live_reload.http_server.close
  exit
end

live_reload.listen
