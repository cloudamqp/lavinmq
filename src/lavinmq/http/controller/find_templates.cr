exit(0) if ARGV.size == 0
path = Path[ARGV[0]].expand
file_extension = ARGV[1]?.to_s.strip
file_extension = ".html" if file_extension.empty?
p = Path[path].join("*#{file_extension}")
puts path
Dir[p].each do |file_path|
  puts file_path
end
