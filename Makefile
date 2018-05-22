build:
	shards build --production --release

install: build
	cp bin/avalanchemq /usr/sbin/

build-deb:
	vagrant up && vagrant ssh -c "cd /vagrant && make deb" && vagrant down

deb:
	build/deb 1

release:
	aws s3 cp avalanchemq_*.deb s3://avalanchemq/
	aws s3 cp `ls avalanchemq_*.deb | tail -1` s3://avalanchemq/avalanchemq_latest.deb
