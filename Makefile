bin/avalanchemq:
	shards install --production
	shards build --release --production avalanchemq

.PHONY: install

install: bin/avalanchemq
	install -s bin/avalanchemq /usr/local/bin/

.PHONY: clean
clean:
	rm bin/*
