# AvalancheMQ Management HTTP API OpenAPI spec

Running `npm ci` will install dependencies, lint and build the documentation.

It also copies it to `static/docs/index.html` so you can view the docs at [http://localhost:15672/docs/](http://localhost:15672/docs/) if you have AvalancheMQ running.

The dependencies:

* [Spectral] is used to lint the documentation. To run it manually: `spectral lint openapi.yaml`
* [ReDoc] is to build the documentation.
  To run it manually: `redoc-cli bundle openapi.yaml` (a file `redoc-static.html` will be created in your working directory)

You can use the [ReDoc Docker image] to watch for updates so you can preview the documentation as you work on it. (Note the gotcha: the browser caches the YAML files even if they have changed, open dev console in the browser to mitigate.)

Start it with `./redoc-serve-and-watch`, then view the docs at [http://localhost:8080/](http://localhost:8080/).

## OpenAPI notes

* `summary` is the short description (used in the redoc menu for instance)
* `description` is a longer description (supports Markdown)

The following script was used to generate the OpenAPI Spec YAML structure

    ruby openapi.rb

[Spectral]: https://github.com/stoplightio/spectral
[ReDoc]: https://github.com/Redocly/redoc
[ReDoc Docker image]: https://github.com/Redocly/redoc/tree/master/config/docker#official-redoc-docker-image
