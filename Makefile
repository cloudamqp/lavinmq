build:
	shards build --production --release

install: build
	cp bin/avalanchemq /usr/sbin/

build-deb:
	vagrant up && vagrant ssh -c "cd /vagrant && build/deb 1" && vagrant halt

deb:
	build/deb 1

release-deb:
	last=$$(ls avalanchemq_*.deb | tail -1) && deb-s3 upload --bucket apt.avalanchemq.com $$last && aws cloudfront create-invalidation --distribution-id E26Y9DFNV7WCMR --paths "/dists/*"
