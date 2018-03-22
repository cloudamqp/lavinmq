build:
	crystal build --release -o bin/amqpserver src/amqpserver.cr

build-linux:
	vagrant up && vagrant ssh -c "cd /vagrant && make build" && vagrant down
