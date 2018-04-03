build:
	shards build --production --release --no-debug

install: build
	cp bin/avalanchemq /usr/sbin/

build-linux:
	vagrant up && vagrant ssh -c "cd /vagrant && make build" && vagrant down
