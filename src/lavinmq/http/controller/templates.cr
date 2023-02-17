require "ecr/macros"

module LavinMQ::HTTP::Templates
  extend self
  Log = ::Log.for("LavinMQ.HTTP.Templates")

  abstract class Template
    Log = Templates::Log.for("Template")
    getter filename

    def initialize(@filename : String, @registry : Registry)
    end

    macro layout(layout_name, vars = NamedTuple(), &block)
      _partial(io, {{layout_name}}, vars) { {{block.body}} }
    end

    macro partial(partial_name, vars = NamedTuple(), &block)
      _partial(io, {{partial_name}},{{vars}}) { {{block.body}} }
    end

    def _partial(io, partial_name, vars, &block)
      if partial = @registry[partial_name]?
        @registry[partial_name].render(io, vars, &block)
      else
        Log.error { "missing partial: #{partial_name}" }
      end
    end

    def render(io, vars = NamedTuple(), layout = "")
      render(io, vars, layout) { }
    end
  end

  class Registry
    Log = Log.for("Registry")

    @templates = Hash(String, Templates::Template).new

    def [](name : String) : Templates::Template
      @templates[name]
    end

    def []?(name : String) : Templates::Template?
      @templates[name]?
    end

    TEMPLATES = [] of NamedTuple(disk_path: String, mapped_path: String, klass: Class)
    HELPERS   = [] of Class

    macro helpers(*helpers)
      {% for h in helpers %}
        {% HELPERS << h %}
      {% end %}
    end

    macro inherited
      {% verbatim do %}
        macro finished
          # Define all template classes
          {% for tpl in TEMPLATES %}
            class {{tpl[:klass].id}} < Templates::Template
              {% for h in HELPERS %}
                include {{h.id}}
              {% end %}
              def render(io, vars, layout_name = "", &)
                ECR.embed {{tpl[:disk_path]}}, io
              end
            end
          {% end %}

          # Define a constructor and populate templates. Also include any previously existing contructor.
          def initialize
            {% for tpl in TEMPLATES %}
              @templates[{{tpl[:mapped_path]}}] = {{@type.name}}::{{tpl[:klass].id}}.new({{tpl[:mapped_path]}}, self)
            {% end %}
            {% if @type.has_method?(:initialize) %}
              previous_def
            {% end %}
          end
        end
        {{debug}}
      {% end %}
    end

    # Add templates to registry from path. Only add files with given extension.
    # Prefix will be appended to the path in the registry, i.e. if you add path
    # ./templates/ and that folder containex tpl.html, tpl.html will be access
    # with /tpl.html. But if you set path: "foo" it will be access with /foo/tpl.html
    #
    macro add_dir(path, extension = ".html", prefix = "", overwrite = false)
      {%
        template_files = run("./find_templates", path, extension).split('\n').map(&.strip).reject(&.empty?)
        file_to_template = {} of String => String
        prefix = prefix.gsub(%r{^/|/$}, "")
        # find_templates will do Path.expand for us, since we can't from macro
        expanded_path = template_files[0]
        template_files[1..].each do |template_file|
          mapped_path = template_file[expanded_path.size..]
          # Remove initial slash, if any
          mapped_path = mapped_path[1..] if mapped_path.starts_with?("/")
          mapped_path = if prefix.empty?
                          "/#{mapped_path.id}"
                        else
                          "/#{mapped_prefix}/#{mapped_path.id}"
                        end

          # e.g. /index.htmlk
          class_name = mapped_path.gsub(/^[^a-z0-9]+/, "").gsub(/[^a-z0-9]+/, "_").camelcase
          class_name = "Template#{class_name.id}"
          file_to_template[mapped_path] = class_name
          TEMPLATES << {disk_path: template_file, mapped_path: mapped_path, klass: class_name}
        end
      %}
    end
  end
end
