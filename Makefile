BINS := bin/avalanchemq bin/avalanchemqctl bin/avalanchemqperf bin/avalanchemq-debug
SOURCES := $(shell find src/avalanchemq src/stdlib -name '*.cr')
JS := static/js/lib/chart.js static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map
DOCS := static/docs/index.html
CRYSTAL_FLAGS := --cross-compile $(if $(target),--target $(target))
LDFLAGS := -rdynamic
LDFLAGS += $(if $(shell crystal version),-L$(shell crystal env CRYSTAL_LIBRARY_PATH))
LDLIBS := -lz -lpcre -lm -lgc -lpthread -levent -ldl
LDLIBS += $(if $(shell pkg-config --version),$(shell pkg-config --libs libssl libcrypto),-lssl -lcrypto)
LDLIBS += $(if $(findstring Linux,$(shell uname)),,-liconv)

.PHONY: all
all: $(BINS)

.PHONY: objects
objects: $(BINS:=.o)

bin/%-debug.o: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $(@:.o=) --debug -Dbake_static $(CRYSTAL_FLAGS) > /dev/null

bin/%.o: src/%.cr $(SOURCES) lib $(JS) $(DOCS) | bin
	crystal build $< -o $(@:.o=) --release --no-debug $(CRYSTAL_FLAGS) > /dev/null

bin/%: bin/%.o
	$(CC) $(LDFLAGS) $< $(LDLIBS) -o $@

lib: shard.yml
	shards install --production

bin static/js/lib:
	mkdir -p $@

static/js/lib/%: | static/js/lib
	wget -qP $(@D) https://github.com/cloudamqp/amqp-client.js/releases/download/v2.0.0/$(@F)

static/js/lib/chart.js: | static/js/lib
	wget -qO- https://github.com/chartjs/Chart.js/releases/download/v2.9.4/chart.js-2.9.4.tgz | \
		tar -zxOf- package/dist/Chart.bundle.min.js > $@

static/docs/index.html: openapi/openapi.yaml $(wildcard openapi/paths/*.yaml) $(wildcard openapi/schemas/*.yaml)
	npx redoc-cli bundle $< -o $@

.PHONY: docs
docs: $(DOCS)

.PHONY: js
js: $(JS)

.PHONY: deps
deps: js lib docs

.PHONY: lint
lint: lib
	lib/ameba/bin/ameba src/

.PHONY: install
install: $(BINS)
	install -s $^ /usr/local/bin/

.PHONY: clean
clean:
	rm -rf bin
	rm -f static/docs/index.html
	rm -f static/js/lib/*
