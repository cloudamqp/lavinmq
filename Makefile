BINS := bin/lavinmq bin/lavinmqctl bin/lavinmqperf
SOURCES := $(shell find src/lavinmq src/stdlib -name '*.cr' 2> /dev/null)
VIEW_SOURCES := $(wildcard views/*.ecr)
VIEW_TARGETS := $(patsubst views/%.ecr,static/views/%.html,$(VIEW_SOURCES))
VIEW_PARTIALS := $(wildcard views/partials/*.ecr)
JS := static/js/lib/chunks/helpers.segment.js static/js/lib/chart.js static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map static/js/lib/luxon.js static/js/lib/chartjs-adapter-luxon.esm.js static/js/lib/elements-8.2.0.js static/js/lib/elements-8.2.0.css
CRYSTAL_FLAGS := --release --stats
override CRYSTAL_FLAGS += --error-on-warnings --link-flags=-pie

.PHONY: livereload
livereload:
	@echo "Starting livereload server..."
	@which livereload > /dev/null || npm install -g livereload
	@(pid=$$!; trap 'kill -TERM $$pid' INT; livereload static &)

.PHONY: views
views: $(VIEW_TARGETS)

.PHONY: watch-views
watch-views:
	while true; do $(MAKE) -q -s views || $(MAKE) -j views; sleep 0.5; done

.PHONY: dev-ui
dev-ui: livereload watch-views

static/views/%.html: views/%.ecr $(VIEW_PARTIALS)
	@mkdir -p static/views
	@TEMP_FILE=$$(mktemp) && \
	INPUT=$< crystal run views/_render.cr > $$TEMP_FILE && \
	mv $$TEMP_FILE $@ && \
	echo "Rendered $< to $@"

.PHONY: clean-views
clean-views:
	$(RM) $(VIEW_TARGETS)

.PHONY: all
all: $(BINS)

bin/%: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $@ $(CRYSTAL_FLAGS)

bin/%-debug: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $@ --debug $(CRYSTAL_FLAGS)

bin/lavinmqperf: src/lavinmqperf.cr lib | bin
	crystal build $< -o $@ -Dpreview_mt $(CRYSTAL_FLAGS)

bin/lavinmqctl: src/lavinmqctl.cr lib | bin
	crystal build $< -o $@ -Dgc_none $(CRYSTAL_FLAGS)

lib: shard.yml shard.lock
	shards install --production $(if $(nocolor),--no-color)

bin static/js/lib man1 static/js/lib/chunks:
	mkdir -p $@

static/js/lib/%: | static/js/lib
	curl --retry 5 -sLo $@ https://github.com/cloudamqp/amqp-client.js/releases/download/v3.1.1/$(@F)

static/js/lib/chart.js: | static/js/lib
	curl --retry 5 -sL https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz | \
		tar -zxOf- package/dist/chart.js > $@

static/js/lib/chunks/helpers.segment.js: | static/js/lib/chunks
	curl --retry 5 -sL https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz | \
		tar -zxOf- package/dist/chunks/helpers.segment.js > $@

static/js/lib/luxon.js: | static/js/lib
	curl --retry 5 -sLo $@ https://moment.github.io/luxon/es6/luxon.js

static/js/lib/chartjs-adapter-luxon.esm.js: | static/js/lib
	curl --retry 5 -sLo $@ https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.esm.js
	sed -i'' -e "s|\(import { _adapters } from\).*|\1 './chart.js'|; s|\(import { DateTime } from\).*|\1 './luxon.js'|" $@

static/js/lib/elements-8.2.0.js: | static/js/lib
	curl --retry 5 -sLo $@ https://unpkg.com/@stoplight/elements@8.2.0/web-components.min.js

static/js/lib/elements-8.2.0.css: | static/js/lib
	curl --retry 5 -sLo $@ https://unpkg.com/@stoplight/elements@8.2.0/styles.min.css

man1/lavinmq.1: bin/lavinmq | man1
	help2man -Nn "fast and advanced message queue server" $< -o $@

man1/lavinmqctl.1: bin/lavinmqctl | man1
	help2man -Nn "control utility for lavinmq server" $< -o $@

man1/lavinmqperf.1: bin/lavinmqperf | man1
	help2man -Nn "performance testing tool for amqp servers" $< -o $@

MANPAGES := man1/lavinmq.1 man1/lavinmqctl.1 man1/lavinmqperf.1

.PHONY: man
man: $(MANPAGES)

.PHONY: js
js: $(JS)

.PHONY: deps
deps: js lib

.PHONY: lint
lint: lib
	lib/ameba/bin/ameba src/

.PHONY: lint-js
lint-js:
	npx standard static/js

.PHONY: lint-openapi
lint-openapi:
	npx --package=@stoplight/spectral-cli spectral --ruleset openapi/.spectral.json lint static/docs/openapi.yaml

.PHONY: test
test: lib
	crystal spec --order random $(if $(nocolor),--no-color) --verbose

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
install: $(BINS) $(MANPAGES) extras/lavinmq.ini extras/lavinmq.service README.md CHANGELOG.md NOTICE
	install -D -m 0755 -t $(DESTDIR)$(BINDIR) $(BINS)
	install -D -m 0644 -t $(DESTDIR)$(MANDIR)/man1 $(MANPAGES)
	install -D -m 0644 extras/lavinmq.ini $(DESTDIR)$(SYSCONFDIR)/lavinmq/lavinmq.ini
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
	$(RM) $(BINS) $(DOCS) $(JS) $(MANPAGES) $(VIEW_TARGETS)
