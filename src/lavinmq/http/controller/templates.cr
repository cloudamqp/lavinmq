require "ecr/macros"

module LavinMQ::HTTP::Templates
  extend self
  Log = Log.for("Templates")

  abstract class Template
    Log = Log.for("Template")
    getter filename

    def initialize(@filename : String, @registry : Registry)
    end

    macro layout(layout_name, vars = NamedTuple(), &block)
      partial({{layout_name}}, vars) { {{block.body}} }
    end

    macro partial(partial_name, vars = NamedTuple(), &block)
      _partial({{partial_name}}, io, {{vars}}) { {{block.body}} }
    end

    def _partial(partial_name, io, vars, &block)
      @registry[partial_name].render(io, vars, &block)
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

    TEMPLATES = {} of String => Templates::Template.class

    macro inherited
      {% verbatim do %}
        macro finished
          def initialize
            {% for f, k in TEMPLATES %}
              @templates[{{f}}] = {{@type.name}}::{{k.id}}.new({{f}}, self)
            {% end %}
            {% if @type.has_method?(:initialize) %}
              previous_def
            {% end %}
          end
        end
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
      %}
      {% for template_file in template_files %}
        {%
          class_name = "Template#{template_file.gsub(/^[^a-z0-9]+/, "").gsub(/[^a-z0-9]+/, "_").camelcase.id}" # file_to_template[template_file] = class_name
          # Remove "path" from the file path
          mapped_path = template_file[path.size..]
          # Remove initial slash, if any
          mapped_path = mapped_path[1..] if mapped_path.starts_with?("/")
          file_to_template[mapped_path] = class_name
        %}
        class {{class_name.id}} < Templates::Template
          def render(io, vars, layout_name = "", &)
            ECR.embed {{template_file}}, io
          end
        end
        add_template({{mapped_path}}, {{class_name}}, {{prefix}}, {{overwrite}})
      {% end %}
    end

    macro add_template(path, class_name, prefix = "", overwrite = false)
      {%
        if prefix.empty?
          file = "/#{path.id}"
        else
          file = "/#{prefix}/#{path.id}"
        end
      %}
      {%
        if TEMPLATES[file] && !overwrite
          Log.warn { "Template {{file.d}} already added" }
        else
          Log.debug { "Registering template {{file.id}}" }
          TEMPLATES[file] = class_name
        end
      %}
    end
  end
end
