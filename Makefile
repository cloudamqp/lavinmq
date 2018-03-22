build:
	shards build --release

build-linux:
	vagrant up && vagrant ssh -c "cd /vagrant && make build" && vagrant down
