# AvalancheMQ Management HTTP API OpenAPI spec

To validate the spec, use [Spectral](https://github.com/stoplightio/spectral).

    npm install -g @stoplight/spectral

    spectral lint openapi.yaml

## Preview the documentation using [Redoc]

Note: Uses [Redoc Docker image].

Serve local file and watch for updates (gotcha: browser caches YAML files even if they have changed, open dev console to mitigate)

    ./redoc-serve-and-watch

## OpenAPI notes

* `summary` is the short description (used in the redoc menu for instance)
* `description` is a longer description (supports Markdown)

The following script was used to generate the OpenAPI Spec YAML structure

    ruby openapi.rb

[Swagger UI]: https://github.com/swagger-api/swagger-ui
[Redoc]: https://github.com/Redocly/redoc
[Redoc Docker image]: https://github.com/Redocly/redoc/tree/master/config/docker#official-redoc-docker-image
