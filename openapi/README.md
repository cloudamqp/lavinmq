# AvalancheMQ Management HTTP API OpenAPI spec

Run `make docs` to build docs (calls `npx redoc-cli`).

It outputs to `static/docs/index.html` so you can view the docs at [http://localhost:15672/docs/](http://localhost:15672/docs/) if you have AvalancheMQ running.

The dependencies:

* [Spectral] is used to lint the documentation. To run it manually: `docker run --rm -it -v $(pwd):/tmp stoplight/spectral:6 --ruleset /tmp/openapi/.spectral.json lint openapi/openapi.yaml`
* [ReDoc] is to build the documentation.
  To run it manually: `npx redoc-cli bundle openapi.yaml -o static/docs/index.html`

You can preview docs using:

```
npx redoc-cli serve openapi.yaml
```

(Note the gotcha: the browser caches the YAML files even if they have changed, open dev console in the browser to mitigate.)

## OpenAPI notes

* `summary` is the short description (used in the redoc menu for instance)
* `description` is a longer description (supports Markdown)

The following script was used to generate the OpenAPI Spec YAML structure

    ruby openapi.rb

[Spectral]: https://github.com/stoplightio/spectral
[ReDoc]: https://github.com/Redocly/redoc
[ReDoc Docker image]: https://github.com/Redocly/redoc/tree/master/config/docker#official-redoc-docker-image
