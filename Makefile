BINS := bin/lavinmq bin/lavinmqctl bin/lavinmqperf
SOURCES := $(shell find src/ -name '*.cr' 2> /dev/null)
PERF_SOURCES := $(shell find src/lavinmqperf -name '*.cr' 2> /dev/null)
VIEW_SOURCES := $(wildcard views/*.ecr)
VIEW_TARGETS := $(patsubst views/%.ecr,static/views/%.html,$(VIEW_SOURCES))
VIEW_PARTIALS := $(wildcard views/partials/*.ecr)
JS := static/js/lib/chunks/helpers.segment.js static/js/lib/chart.js static/js/lib/luxon.js static/js/lib/chartjs-adapter-luxon.esm.js static/js/lib/elements-8.2.0.js static/js/lib/elements-8.2.0.css
LDFLAGS := $(shell (dpkg-buildflags --get LDFLAGS || rpm -E "%{build_ldflags}" || echo "-pie") 2>/dev/null)
CRYSTAL_FLAGS := --release
override CRYSTAL_FLAGS += --stats --error-on-warnings -Dpreview_mt -Dexecution_context --link-flags="$(LDFLAGS)"

.DEFAULT_GOAL := all

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
dev-ui:
	@trap '$(MAKE) clean-views; trap - EXIT' EXIT INT TERM; \
	 $(MAKE) livereload & \
	 livereload_pid=$$!; \
	 $(MAKE) -s watch-views; \
	 wait $$livereload_pid

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

bin/lavinmqctl: src/lavinmqctl.cr lib | bin
	crystal build $< -o $@ -Dgc_none $(CRYSTAL_FLAGS)

lib: shard.yml shard.lock
	shards install --production

bin static/js/lib man1 static/js/lib/chunks:
	mkdir -p $@

static/js/lib/chart.js: | static/js/lib
	curl --fail --retry 5 -sL -o chart.js.tgz https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz && \
		(echo "461dae2edc0eda7beeb16c7030ab630ab5129aedd3fc6de9a036f6dfe488556f chart.js.tgz" | sha256sum -c - && \
		tar -zxOf chart.js.tgz package/dist/chart.js > $@ ; \
		(rm chart.js.tgz && echo "removed chart.js.tgz")

static/js/lib/chunks/helpers.segment.js: | static/js/lib/chunks
	curl --fail --retry 5 -sL -o chart.js.tgz https://github.com/chartjs/Chart.js/releases/download/v4.0.1/chart.js-4.0.1.tgz && \
		echo "461dae2edc0eda7beeb16c7030ab630ab5129aedd3fc6de9a036f6dfe488556f chart.js.tgz" | sha256sum -c - && \
		tar -zxOf chart.js.tgz package/dist/chunks/helpers.segment.js > $@ ; \
		(rm chart.js.tgz && echo "removed chart.js.tgz")

static/js/lib/luxon.js: | static/js/lib
	curl --fail --retry 5 -sLo $@ https://moment.github.io/luxon/es6/luxon.mjs && \
		echo "b495ad5cabea3439d04387e6622f2c3fa81d319424d9d76d9e7f874ac5a0807a $@" | \
		sha256sum -c - || (echo "SHA256 checksum mismatch for $@"; rm -f $@; exit 1)

static/js/lib/chartjs-adapter-luxon.esm.js: | static/js/lib
	curl --fail --retry 5 -sLo $@ https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.esm.js && \
	echo "fa02364f717191a48067215aaf9ff93b54ff52e2de64704270742e1d15d1b6df $@" | sha256sum -c - && \
	sed -i'' -e "s|\(import { _adapters } from\).*|\1 './chart.js'|; s|\(import { DateTime } from\).*|\1 './luxon.js'|" $@ || \
	(echo "SHA256 checksum mismatch for $@"; rm -f $@; exit 1) ;

static/js/lib/elements-8.2.0.js: | static/js/lib
	curl --fail --retry 5 -sLo $@ https://unpkg.com/@stoplight/elements@8.2.0/web-components.min.js && \
	echo "598862da6d551769ebad9d61d4e3037535de573a13d3e0bd1ded4c5fc65c5885 $@" | sha256sum -c - || \
	(echo "SHA256 checksum mismatch for $@"; rm -f $@; exit 1)

static/js/lib/elements-8.2.0.css: | static/js/lib
	curl --fail --retry 5 -sLo $@ https://unpkg.com/@stoplight/elements@8.2.0/styles.min.css && \
	echo "119784e23ffc39b6fa3fdb3df93f391f8250e8af141b78dfc3b6bed86079f93b $@" | sha256sum -c - || \
	(echo "SHA256 checksum mismatch for $@"; rm -f $@; exit 1)

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
	lib/ameba/bin/ameba src/ spec/

.PHONY: lint-js
lint-js:
	npx standard static/js

.PHONY: lint-openapi
lint-openapi:
	npx --package=@stoplight/spectral-cli spectral --ruleset openapi/.spectral.json lint static/docs/openapi.yaml

.PHONY: test
test: lib
	crystal spec --order random --verbose -Dpreview_mt -Dexecution_context $(SPEC)

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
	getent passwd lavinmq >/dev/null || useradd --system --user-group --home $(SHAREDSTATEDIR)/lavinmq lavinmq
	install -d -m 0750 -o lavinmq -g lavinmq $(DESTDIR)$(SHAREDSTATEDIR)/lavinmq

.PHONY: uninstall
uninstall:
	$(RM) $(DESTDIR)$(BINDIR)/lavinmq{,ctl,perf}
	$(RM) $(DESTDIR)$(MANDIR)/man1/lavinmq{,ctl,perf}.1
	$(RM) $(DESTDIR)$(SYSCONFDIR)/lavinmq/lavinmq.ini
	$(RM) $(DESTDIR)$(UNITDIR)/lavinmq.service
	$(RM) -r $(DESTDIR)$(DOCDIR)/lavinmq

.PHONY: rpm
rpm:
	rpmdev-setuptree
	git archive --prefix lavinmq/ --output ~/rpmbuild/SOURCES/lavinmq.tar.gz HEAD
	env version="$(shell ./packaging/rpm/rpm-version)" rpmbuild -bb packaging/rpm/lavinmq.spec

.PHONY: clean
clean:
	$(RM) $(BINS) $(DOCS) $(JS) $(MANPAGES) $(VIEW_TARGETS)

.PHONY: watch
watch:
	while true; do inotifywait -qqr -e modify -e create -e delete src/ && $(MAKE); done
