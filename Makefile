SOURCES := $(shell find src -name '*.cr')

bin/avalanchemq: $(SOURCES) lib | js bin
	crystal build src/avalanchemq.cr -o $@ $(FLAGS)

bin/avalanchemqperf: $(SOURCES) lib | bin
	crystal build src/avalanchemqperf.cr -o $@ $(FLAGS)

bin/avalanchemqctl: $(SOURCES) lib | bin
	crystal build src/avalanchemqctl.cr -o $@ $(FLAGS)

lib: shard.yml shard.lock
	shards install $(FLAGS)

bin:
	mkdir $@

static/js/lib:
	mkdir $@

static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map: | static/js/lib
	wget -qP $(@D) https://github.com/cloudamqp/amqp-client.js/releases/download/v2.0.0/$(@F)

static/js/lib/chart.js: | static/js/lib
	wget -qO- https://github.com/chartjs/Chart.js/releases/download/v3.7.1/chart.js-3.7.1.tgz | tar zx -C /tmp package/dist/chart.js
	mv /tmp/package/dist/chart.js $@

.PHONY: js
js: static/js/lib/amqp-websocket-client.mjs static/js/lib/amqp-websocket-client.mjs.map static/js/lib/chart.js

.PHONY: all
all: bin/avalanchemq bin/avalanchemqctl bin/avalanchemqperf

.PHONY: install
install: bin/avalanchemq
	install -s bin/avalanchemq /usr/local/bin/

.PHONY: clean
clean:
	rm -f bin/*
