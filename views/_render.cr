require "ecr/macros"
require "../src/lavinmq/version"

# Used by `make dev-ui` to render views to static files for faster frontend development.
{% begin %}
  {%
    source = env("INPUT").gsub(/views\/(.*)\.ecr/, "\\1")
  %}

  macro render(file)
    ECR.embed("views/\{{file.id}}.ecr", STDOUT)
  end

  macro active_path?(path)
    local_path = if "#{\{{path}}}" == "."
      "overview"
    else
      \{{path}}
    end

    "#{local_path}" == {{source}}
  end

  render({{source}})
{% end %}
