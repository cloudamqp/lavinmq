build:
	crystal build --release -o bin/avalanchemq src/avalanchemq.cr

build-linux:
	vagrant up && vagrant ssh -c "cd /vagrant && make build" && vagrant down
