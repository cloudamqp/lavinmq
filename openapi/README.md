# LavinMQ HTTP API OpenAPI spec

OpenAPI docs rendered in browser using https://github.com/stoplightio/elements

(Note the gotcha: the browser caches the YAML files even if they have changed, open dev console in the browser to mitigate.)

## OpenAPI notes

* `summary` is the short description
* `description` is a longer description (supports Markdown)

The following script was used to generate the OpenAPI Spec YAML structure

    ruby openapi.rb

