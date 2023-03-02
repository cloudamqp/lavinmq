BINS := bin/lavinmq bin/lavinmqctl bin/lavinmqperf
SOURCES := $(shell find src/lavinmq src/stdlib -name '*.cr' 2> /dev/null)
JS := static/js/lib/chunks/helpers.segment.js static/js/lib/chart.js static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map static/js/lib/luxon.js static/js/lib/chartjs-adapter-luxon.esm.js
DOCS := static/docs/index.html
CRYSTAL_FLAGS := --release
SPECS_COMBINED := $(shell mktemp -t lavinmq_specs)
override CRYSTAL_FLAGS += --error-on-warnings --link-flags=-pie

.PHONY: all
all: $(BINS)

bin/%: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $@ $(CRYSTAL_FLAGS)

bin/%-debug: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $@ --debug $(CRYSTAL_FLAGS)

bin/lavinmqperf: src/lavinmqperf.cr lib | bin
	crystal build $< -o $@ -Dpreview_mt $(CRYSTAL_FLAGS)

bin/lavinmqctl: src/lavinmqctl.cr lib | bin
	crystal build $< -o $@ $(CRYSTAL_FLAGS)

#bin/specs: spec/combined.cr
#	crystal build $< -o $@ $(CRYSTAL_FLAGS)
#
#spec/combined.cr: lib
#	find spec -name "*_spec.cr" -type f -exec cat '{}' + | sed 's/^require \".*\/spec_helper/require \"\.\/spec_helper/' |  sed 's/^require \".*\/src/require \"\.\.\/src/' > spec/combined.cr

bin/specs: $(SPECS_COMBINED)
	crystal build $< -o $@ $(CRYSTAL_FLAGS)

$(SPECS_COMBINED): lib
	find spec -name "*_spec.cr" -type f -exec cat '{}' + | sed 's/^require \".*\/spec_helper/require \"\.\/spec_helper/' |  sed 's/^require \".*\/src/require \"\.\.\/src/' > $(SPECS_COMBINED)

lib: shard.yml shard.lock
	shards install --production $(if $(nocolor),--no-color)

bin static/js/lib man1 static/js/lib/chunks:
	mkdir -p $@

static/js/lib/%: | static/js/lib
	curl --retry 5 -sLo $@ https://github.com/cloudamqp/amqp-client.js/releases/download/v2.1.0/$(@F)

static/js/lib/chart.js: | static/js/lib
	curl --retry 5 -sL https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz | \
		tar -zxOf- package/dist/chart.js > $@

static/js/lib/chunks/helpers.segment.js: | static/js/lib/chunks
	curl --retry 5 -sL https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz | \
		tar -zxOf- package/dist/chunks/helpers.segment.js > $@

static/js/lib/luxon.js: | static/js/lib
	curl --retry 5 -sLo $@ https://moment.github.io/luxon/es6/luxon.js

static/js/lib/chartjs-adapter-luxon.esm.js: | static/js/lib
	curl --retry 5 -sLo $@ https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.0/dist/chartjs-adapter-luxon.esm.js
	sed -i'' -e "s|\(import { _adapters } from\).*|\1 './chart.js'|; s|\(import { DateTime } from\).*|\1 './luxon.js'|" $@

static/docs/index.html: openapi/openapi.yaml openapi/.spectral.json $(wildcard openapi/paths/*.yaml) $(wildcard openapi/schemas/*.yaml)
	npx --package=@stoplight/spectral-cli spectral --ruleset openapi/.spectral.json lint $<
	npx redoc-cli build --output $@ $<

man1/lavinmq.1: bin/lavinmq | man1
	help2man -Nn "fast and advanced message queue server" $< -o $@

man1/lavinmqctl.1: bin/lavinmqctl | man1
	help2man -Nn "control utility for lavinmq server" $< -o $@

man1/lavinmqperf.1: bin/lavinmqperf | man1
	help2man -Nn "performance esting tool for amqp servers" $< -o $@

MANPAGES := man1/lavinmq.1 man1/lavinmqctl.1 man1/lavinmqperf.1

.PHONY: man
man: $(MANPAGES)

.PHONY: docs
docs: $(DOCS)

.PHONY: js
js: $(JS)

.PHONY: deps
deps: js lib docs

.PHONY: lint
lint: lib
	lib/ameba/bin/ameba src/

.PHONY: test
test: lib
	crystal spec --order random $(if $(nocolor),--no-color) $(if $(progress),--progress)

.PHONY: test-compiled
test-compiled: bin/specs
	./bin/specs

.PHONY: format
format:
	crystal tool format --check

DESTDIR :=
PREFIX := /usr
BINDIR := $(PREFIX)/bin
DOCDIR := $(PREFIX)/share/doc
MANDIR := $(PREFIX)/share/man
SYSCONFDIR := /etc
UNITDIR := /lib/systemd/system
SHAREDSTATEDIR := /var/lib

.PHONY: install
install: $(BINS) $(MANPAGES) extras/config.ini extras/lavinmq.service README.md CHANGELOG.md NOTICE
	install -D -m 0755 -t $(DESTDIR)$(BINDIR) $(BINS)
	install -D -m 0644 -t $(DESTDIR)$(MANDIR)/man1 $(MANPAGES)
	install -D -m 0644 extras/config.ini $(DESTDIR)$(SYSCONFDIR)/lavinmq/lavinmq.ini
	install -D -m 0644 extras/lavinmq.service $(DESTDIR)$(UNITDIR)/lavinmq.service
	install -D -m 0644 -t $(DESTDIR)$(DOCDIR)/lavinmq README.md NOTICE
	install -D -m 0644 CHANGELOG.md $(DESTDIR)$(DOCDIR)/lavinmq/changelog
	install -d -m 0755 $(DESTDIR)$(SHAREDSTATEDIR)/lavinmq

.PHONY: uninstall
uninstall:
	$(RM) $(DESTDIR)$(BINDIR)/lavinmq{,ctl,perf}
	$(RM) $(DESTDIR)$(MANDIR)/man1/lavinmq{,ctl,perf}.1
	$(RM) $(DESTDIR)$(SYSCONFDIR)/lavinmq/lavinmq.ini
	$(RM) $(DESTDIR)$(UNITDIR)/lavinmq.service
	$(RM) $(DESTDIR)$(DOCDIR)/{lavinmq,README.md,CHANGELOG.md,NOTICE}
	$(RM) $(DESTDIR)$(SHAREDSTATEDIR)/lavinmq

.PHONY: clean
clean:
	$(RM) $(BINS) $(DOCS) $(JS) $(MANPAGES)
