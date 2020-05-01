module AvalancheMQ
  VERSION = {{ `git describe 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}
end
