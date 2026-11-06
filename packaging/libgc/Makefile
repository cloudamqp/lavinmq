# bdwgc with mark-stack-overflow.patch, built as a static library in
# bdwgc/.libs. Same configure flags as the 84codes/crystal images.
VERSION := 8.2.12
SHA256 := 42e5194ad06ab6ffb806c83eb99c03462b495d979cda782f3c72c08af833cd4e
GC_CFLAGS := -fPIC -pipe -O2 -std=gnu17
.DELETE_ON_ERROR:

# mark_stack_test fails if the patch doesn't take effect
bdwgc/.libs/libgc.a: gc-$(VERSION).tar.gz mark-stack-overflow.patch mark_stack_test.c Makefile
	$(RM) -r bdwgc
	mkdir bdwgc
	tar -xzf $< -C bdwgc --strip-components=1
	patch -s -d bdwgc -p1 < mark-stack-overflow.patch
	cd bdwgc && CFLAGS="$(GC_CFLAGS)" ./configure -q --disable-debug --disable-shared --enable-large-config
	$(MAKE) -s -C bdwgc >/dev/null
	$(CC) -O2 -pthread -DHAVE_CONFIG_H -Ibdwgc -Ibdwgc/include mark_stack_test.c $@ -o bdwgc/mark_stack_test
	bdwgc/mark_stack_test

gc-$(VERSION).tar.gz:
	curl --fail --retry 5 -sLo $@ https://github.com/ivmai/bdwgc/releases/download/v$(VERSION)/$@
	[ "$(SHA256) *$@" = "$$(openssl dgst -sha256 -r $@)" ]

.PHONY: clean
clean:
	$(RM) -r bdwgc gc-*.tar.gz
