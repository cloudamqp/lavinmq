require "./version"
require "log"

module LavinMQ
  module Deprecation
    Log = ::Log.for("deprecation")

    @@features = [] of NamedTuple(name: String, deprecated_in: String, remove_in: String, message: String)

    macro deprecated(name, deprecated_in, remove_in, message)
      {% if compare_versions(LavinMQ::VERSION, remove_in) >= 0 %}
        {% raise "DEPRECATION ERROR: '#{name.id}' was marked for removal in #{remove_in.id} but is still present in version #{LavinMQ::VERSION.id}. #{message.id}" %}
      {% end %}

      LavinMQ::Deprecation.add({{ name }}, {{ deprecated_in }}, {{ remove_in }}, {{ message }})
    end

    def self.add(name : String, deprecated_in : String, remove_in : String, message : String)
      @@features << {name: name, deprecated_in: deprecated_in, remove_in: remove_in, message: message}
    end

    def self.log_startup_warnings
      return if @@features.empty?
      Log.warn { "This version contains #{@@features.size} deprecated feature(s):" }
      @@features.each do |f|
        Log.warn { "  - #{f[:name]} (deprecated in #{f[:deprecated_in]}, will be removed in #{f[:remove_in]}): #{f[:message]}" }
      end
    end
  end
end
