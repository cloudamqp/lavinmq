# AvalancheMQ Management HTTP API OpenAPI spec

The following script was used to generate the OpenAPI Spec YAML structure

    ruby openapi.rb

To work on the documentation

1. Run `npm install` to prepare the `static/docs` directory
1. Start AvalancheMQ (not in release mode)
1. Open [http://localhost:15672/docs/dev-index.html](http://localhost:15672/docs/dev-index.html)

To validate the spec, use [spectral](https://github.com/stoplightio/spectral)

    npm install -g @stoplight/spectral

    spectral lint openapi.yaml

OpenAPI notes:

* `summary` is the short description (used in the redoc menu for instance)
* `description` is a longer description (supports Markdown)
