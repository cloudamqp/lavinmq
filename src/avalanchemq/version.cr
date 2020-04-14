module AvalancheMQ
  VERSION = {{ `git describe | cut -c2-`.chomp.stringify }}
end
