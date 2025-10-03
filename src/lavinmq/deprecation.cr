require "./version"
require "log"

module LavinMQ
  module Deprecation
    Log = ::Log.for("deprecation")

    @@features = [] of NamedTuple(name: String, deprecated_in: String, remove_in: String, message: String)

    macro deprecated(name, deprecated_in, remove_in, message)
      {% current_version = `[ -n "$version" ] && echo "$version" || git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") %}
      {% if current_version.split(".")[0].to_i >= remove_in.split(".")[0].to_i %}
        {% raise "DEPRECATION ERROR: '#{name.id}' was marked for removal in #{remove_in.id} but is still present in version #{current_version.id}. #{message.id}" %}
      {% end %}

      LavinMQ::Deprecation.add({{ name }}, {{ deprecated_in }}, {{ remove_in }}, {{ message }})
    end

    def self.add(name : String, deprecated_in : String, remove_in : String, message : String)
      @@features << {name: name, deprecated_in: deprecated_in, remove_in: remove_in, message: message}
    end

    # def self.warn(feature_name : String, deprecated_in : String, remove_in : String, message : String)
    #   Log.warn { "DEPRECATED: '#{feature_name}' was deprecated in #{deprecated_in} and will be removed in #{remove_in}. #{message}" }
    # end

    def self.log_startup_warnings
      return if @@features.empty?
      Log.warn { "This version contains #{@@features.size} deprecated feature(s):" }
      @@features.each do |f|
        Log.warn { "  - #{f[:name]} (deprecated in #{f[:deprecated_in]}, will be removed in #{f[:remove_in]}): #{f[:message]}" }
      end
    end
  end
end
