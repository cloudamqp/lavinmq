require "yaml"

DEBUG = ENV["DEBUG"] != "false" ? true : false
WRITE = ENV.fetch("WRITE", false)

module Helper
  module_function

  def basename(src_file)
    File.basename(src_file, ".cr")
  end

  def paths_path(src_file)
    File.join(".", "paths", "#{basename(src_file)}.yaml")
  end

  def schemas_path(src_file)
    File.join(".", "schemas", "#{basename(src_file)}.yaml")
  end
end

Route = Struct.new(:route, :verb, :src_file) do
  def to_openapi
    {
      "tags" => [tag],
      "description" => "CHANGEME_LONG.",
      "summary" => "CHANGEME_SHORT",
      "parameters" => path_parameters,
      "operationId" => "CHANGEME-#{verb.capitalize}#{tag.capitalize}",
      "responses" => {
        "200" => {
          "description" => "OK",
          "content" => {
            "application/json" => {
              "schema" => {
                "$ref" => "../openapi.yaml#/components/schemas/#{tag}"
              }
            }
          }
        },
        "4XX" => {
          "description" => "Client Error",
          "content" => {
            "application/json" => {
              "schema" => {
                "$ref" => "../openapi.yaml#/components/schemas/ErrorResponse"
              }
            }
          },
        },
        "5XX" => {
          "description" => "Server Error",
          "content" => {
            "application/json" => {
              "schema" => {
                "$ref" => "../openapi.yaml#/components/schemas/ErrorResponse"
              }
            }
          },
        },
      },
    }
  end

  def path_parameters
    parameters = route.scan(/(:|\*)(\w+)/).map(&:last)

    parameters.map do |parameter|
      {
        "in" => "path",
        "name" => parameter,
        "required" => true,
        "schema" => {
          "type" => "string",
          "description" => "CHANGEME.",
        },
      }
    end
  end

  # https://swagger.io/docs/specification/describing-parameters/
  # Handle both named parameters and glob parameters
  # https://github.com/luislavena/radix/blob/v0.3.9/src/radix/tree.cr#L251-L291
  def with_path_param
    route.gsub(/:(\w+)/, '{\1}').gsub(/\*(\w+)/, '{\1}')
  end

  def spec_path
    Helper.paths_path(src_file)
  end

  def tag
    Helper.basename(src_file)
  end

  # See "Escape Characters" at https://swagger.io/docs/specification/using-ref/
  def ref
    spec_path + "#/" + with_path_param.gsub("~", "~0").gsub("/", "~1")
  end
end

docs_path = File.join("..", "static", "docs")
openapi_spec = YAML.load(File.read(File.join(__dir__, docs_path, "openapi.yaml")))
src_code_dir = File.expand_path(File.join(__dir__, ".."))
http_code_dir = File.join(src_code_dir, "src/lavinmq/http")
files = Dir.glob(File.join(http_code_dir, "**/*.cr"))
route_regex = /(?<verb>get|post|put|delete)\s+"\/api(?<route>\/.+)"/i

files_with_api_routes = Hash.new { |hash, key| hash[key] = [] }

# sort so thing doesn't moved around
files.sort.each do |file|
  File.readlines(file).each do |line|
    line.match(route_regex) do |matches|
      route = Route.new(matches["route"], matches["verb"], file)

      files_with_api_routes[file] << route
    end
  end
end

# because we read the existing file
openapi_spec["paths"] = {}

# tags
existing_tags = openapi_spec.fetch("tags", [])
tag_names_from_src = files_with_api_routes.map { |src_file, _| Helper.basename(src_file) }

# keep existing tag objects
tags = existing_tags.select { |tag| tag_names_from_src.include?(tag.fetch("name")) }

# add new tags from source code
tags += tag_names_from_src.map do |tag_name|
  if tags.none? { |tag| tag.fetch("name") ==  tag_name }
    { "name" => tag_name, "description" => "CHANGEME" }
  end
end.compact

# schemas
schemas = openapi_spec.dig("components", "schemas")

# add one schema per tag
tag_names_from_src.each do |tag_name|
  openapi_schemas = {
    tag_name => {
      "title" => "CHANGEME",
      "type" => "object",
      "properties" => {
        "CHANGEME" => {
          "type" => "string",
          "description" => "CHANGEME",
        }
      }
    }
  }

  if WRITE
    File.write(File.join(__dir__, docs_path, Helper.schemas_path(tag_name)), YAML.dump(openapi_schemas))
  end

  schemas[tag_name] = { "$ref" => "./schemas/#{tag_name}.yaml#/#{tag_name}" }
end

files_with_api_routes.each do |src_file, api_routes|
  openapi_routes = Hash.new { |hash, key| hash[key] = {} }

  api_routes.each do |route|
    puts "#{route.verb.upcase}\t#{route.with_path_param}" if DEBUG

    openapi_routes[route.with_path_param][route.verb] = route.to_openapi
    openapi_spec["paths"][route.with_path_param] = { "$ref" => route.ref }
  end

  if WRITE
    File.write(File.join(__dir__, docs_path, Helper.paths_path(src_file)), YAML.dump(openapi_routes))
  end
end

openapi_spec["tags"] = tags
openapi_spec["components"]["schemas"] = schemas

puts YAML.dump(openapi_spec) if DEBUG

if WRITE
  File.write(File.join(__dir__, docs_path, "openapi.yaml"), YAML.dump(openapi_spec))
end
