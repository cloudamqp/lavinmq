module AvalancheMQ
  VERSION = {{ `git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}
end
