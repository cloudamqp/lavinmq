# Bundles a modular OpenAPI spec into a single YAML file by inlining
# external $refs (./paths/*.yaml, ./schemas/*.yaml) and rewriting
# inward refs (../openapi.yaml#/...) to local anchors (#/...).
#
# Usage: crystal run openapi/bundle.cr -- <source.yaml> <bundled.yaml>

require "yaml"
require "json"

class Bundler
  @cache = {} of String => JSON::Any

  def initialize(@root_dir : String)
  end

  def bundle(source : String) : JSON::Any
    doc = load(source)
    rewrite(doc, File.dirname(source))
  end

  private def load(path : String) : JSON::Any
    absolute = File.expand_path(path)
    @cache[absolute] ||= JSON.parse(YAML.parse(File.read(absolute)).to_json)
  end

  private def resolve(doc : JSON::Any, pointer : String) : JSON::Any
    return doc if pointer.empty? || pointer == "/"
    parts = pointer.lchop('/').split('/').map { |p| p.gsub("~1", "/").gsub("~0", "~") }
    parts.reduce(doc) do |acc, part|
      case raw = acc.raw
      when Hash  then acc[part]
      when Array then acc[part.to_i]
      else            raise "cannot descend into #{raw.class} with #{part.inspect}"
      end
    end
  end

  private def rewrite(value : JSON::Any, base_dir : String) : JSON::Any
    case raw = value.raw
    when Hash(String, JSON::Any)
      if raw.size == 1 && (ref = raw["$ref"]?)
        return inline(ref.as_s, base_dir)
      end
      new_hash = {} of String => JSON::Any
      raw.each { |k, v| new_hash[k] = rewrite(v, base_dir) }
      JSON::Any.new(new_hash)
    when Array(JSON::Any)
      JSON::Any.new(raw.map { |v| rewrite(v, base_dir).as(JSON::Any) })
    else
      value
    end
  end

  private def inline(ref : String, base_dir : String) : JSON::Any
    # Already a local anchor — nothing to do.
    return JSON::Any.new({"$ref" => JSON::Any.new(ref)}) if ref.starts_with?('#')

    file_part, _, pointer = ref.partition('#')
    target_path = File.expand_path(file_part, base_dir)

    # Refs pointing back at the root spec become local anchors.
    if target_path == File.expand_path(@root_dir)
      return JSON::Any.new({"$ref" => JSON::Any.new("#" + pointer)})
    end

    target = resolve(load(target_path), pointer)
    rewrite(target, File.dirname(target_path))
  end
end

source = ARGV[0]? || abort "usage: bundle.cr <source.yaml> <output.yaml>"
output = ARGV[1]? || abort "usage: bundle.cr <source.yaml> <output.yaml>"

bundled = Bundler.new(source).bundle(source)
File.write(output, bundled.to_yaml)
