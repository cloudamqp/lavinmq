WASI_SYSROOT ?= /tmp/wasi-sdk-32.0-arm64-macos/share/wasi-sysroot
WASM_LD      ?= $(shell brew --prefix lld)/bin/wasm-ld

.PHONY: build spec serve

build:
	PATH="$(dir $(WASM_LD)):$$PATH" crystal build \
		--target wasm32-wasi \
		src/main.cr \
		-o demo/lavinmq-router.wasm \
		--link-flags "-L$(WASI_SYSROOT)/lib/wasm32-wasi"

spec:
	crystal spec spec/router_spec.cr

serve:
	cd demo && python3 -m http.server 8765
