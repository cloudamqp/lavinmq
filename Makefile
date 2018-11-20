build:
	shards build --production --release

install: build
	cp bin/avalanchemq /usr/sbin/

build-deb:
	vagrant up && vagrant ssh -c "cd /vagrant && build/deb 1" && vagrant halt

deb:
	build/deb 1

release-deb:
	deb-s3 upload --bucket apt.avalanchemq.com builds/avalanchemq_*.deb && aws cloudfront create-invalidation --distribution-id E26Y9DFNV7WCMR --paths "/dists/*"
