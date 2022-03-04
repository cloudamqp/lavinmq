BINS := bin/avalanchemq bin/avalanchemqctl bin/avalanchemqperf bin/avalanchemq-debug
SOURCES := $(shell find src/avalanchemq src/stdlib -name '*.cr')
CRYSTAL_FLAGS := --cross-compile $(if $(target),--target $(target))
LDFLAGS := -rdynamic -L$(shell crystal env CRYSTAL_LIBRARY_PATH)
LDLIBS := -lz -lssl -lcrypto -lpcre -lm -lgc -lpthread -levent -lrt -ldl

.PHONY: all
all: $(BINS)

.PHONY: objects
objects: $(BINS:=.o)

bin/%-debug.o: src/%.cr $(SOURCES) lib js docs | bin
	crystal build $< -o $(@:.o=) --debug $(CRYSTAL_FLAGS) > /dev/null

bin/%.o: src/%.cr $(SOURCES) lib js docs | bin
	crystal build $< -o $(@:.o=) --release --no-debug $(CRYSTAL_FLAGS) > /dev/null

bin/%: bin/%.o
	$(CC) $< -o $@ $(LDFLAGS) $(LDLIBS)

lib: shard.yml
	shards install --production

bin static/js/lib:
	mkdir $@

static/js/lib/%: | static/js/lib
	wget -qP $(@D) https://github.com/cloudamqp/amqp-client.js/releases/download/v2.0.0/$(@F)

static/js/lib/chart.js: | static/js/lib
	wget -qO- https://github.com/chartjs/Chart.js/releases/download/v2.9.4/chart.js-2.9.4.tgz | \
		tar zx package/dist/Chart.bundle.min.js -O > $@

static/docs/index.html: openapi/openapi.yaml
	npx redoc-cli bundle $< -o $@

.PHONY: docs
docs: static/docs/index.html

.PHONY: js
js: static/js/lib/chart.js static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map

.PHONY: install
install: $(BINS)
	install -s $^ /usr/local/bin/

.PHONY: clean
clean:
	rm -rf bin
	rm -f static/docs/index.html
	rm -f static/js/lib/*
