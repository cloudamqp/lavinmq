# libgc from https://github.com/cloudamqp/bdwgc, built as a static library
# in bdwgc/.libs. Same configure flags as the 84codes/crystal images.
VERSION := 8.2.12-lavinmq.1
SHA256 := ddb405ce4fb08cb3fe8b3759da028e5067c6316cae1936fdc9cb34c16d74a1d4
GC_CFLAGS := -fPIC -pipe -O2 -std=gnu17
.DELETE_ON_ERROR:

bdwgc/.libs/libgc.a: gc-$(VERSION).tar.gz Makefile
	$(RM) -r bdwgc
	mkdir bdwgc
	tar -xzf $< -C bdwgc --strip-components=1
	cd bdwgc && CFLAGS="$(GC_CFLAGS)" ./configure -q --disable-debug --disable-shared --enable-large-config
	$(MAKE) -s -C bdwgc >/dev/null

gc-$(VERSION).tar.gz:
	curl --fail --retry 5 -sLo $@ https://github.com/cloudamqp/bdwgc/releases/download/v$(VERSION)/$@
	[ "$(SHA256) *$@" = "$$(openssl dgst -sha256 -r $@)" ]

.PHONY: clean
clean:
	$(RM) -r bdwgc gc-*.tar.gz
