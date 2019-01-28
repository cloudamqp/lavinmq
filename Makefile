bin/avalanchemq:
	crystal build --release -o bin/avalanchemq src/avalanchemq.cr

.PHONY: install

install: bin/avalanchemq
	install -s bin/avalanchemq /usr/local/bin/

.PHONY: clean
clean:
	rm bin/*
