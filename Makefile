SOURCES := $(shell find src -name '*.cr')
JS := static/js/lib/amqp-websocket-client.mjs \
			static/js/lib/amqp-websocket-client.mjs.map \
			static/js/lib/chart.js
BINS := bin/avalanchemq bin/avalanchemqctl bin/avalanchemqperf

.PHONY: all
all: $(BINS)

bin/%: $(SOURCES) lib $(JS) | bin
	crystal build src/$(@F).cr -o $@ $(FLAGS)

lib: shard.yml shard.lock
	shards install $(FLAGS)

bin static/js/lib:
	mkdir $@

static/js/lib/%: | static/js/lib
	wget -qP $(@D) https://github.com/cloudamqp/amqp-client.js/releases/download/v2.0.0/$(@F)

static/js/lib/chart.js: | static/js/lib
	wget -qO- https://github.com/chartjs/Chart.js/releases/download/v3.7.1/chart.js-3.7.1.tgz | \
		tar zx -C $(@D) --strip-components=2 package/dist/chart.js

.PHONY: js
js: $(JS)

.PHONY: install
install: $(BINS)
	install -s $^ /usr/local/bin/

.PHONY: clean
clean:
	rm -rf bin
