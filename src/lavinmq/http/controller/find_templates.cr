# Usage: find_templates <path> [include pattern] [exclude pattern]
exit(0) if ARGV.size == 0
path = Path[ARGV[0]].expand
include_pattern = /./
exclude_pattern = /^$/
if pattern = ARGV[1]?
  include_pattern = Regex.new(pattern)
end
if pattern = ARGV[2]?
  exclude_pattern = Regex.new(pattern)
end

p = Path[path].join("**", "*")
puts path
Dir[p].each do |file_path|
  next if Dir.exists?(file_path)
  next if exclude_pattern.matches?(file_path)
  next if !include_pattern.matches?(file_path)
  puts file_path
end
