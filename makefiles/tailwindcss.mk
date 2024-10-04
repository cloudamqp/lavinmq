TW_VERSION := 3.4.13
# Download and commit the checksum file when updating the version (make makefiles/tailwind-sha256sums.txt)
TW_CHECKSUM_FILE := makefiles/tailwind-sha256sums.txt
TW_BASE_URL := https://github.com/tailwindlabs/tailwindcss/releases/download/v$(TW_VERSION)

# Determine the OS and architecture
ifeq ($(shell uname -s), Linux)
	TW_OS := linux
endif
ifeq ($(shell uname -s), Darwin)
	TW_OS := macos
endif

TW_ARCH := $(shell uname -m)
ifeq ($(TW_ARCH), x86_64)
	TW_ARCH := x64
endif
ifeq ($(TW_ARCH), aarch64)
	TW_ARCH := arm64
endif

# The target binary and checksum
TW_BINARY := tailwindcss-$(TW_OS)-$(TW_ARCH)
TW_DOWNLOAD_URL := $(TW_BASE_URL)/$(TW_BINARY)
TW_CHECKSUM_URL := $(TW_BASE_URL)/sha256sums.txt

# Targets
tailwind: bin/tailwindcss

bin/tailwindcss: | bin
	@echo "Downloading Tailwind CSS binary for $(TW_OS)-$(TW_ARCH)..."
	curl --retry 5 -sLo bin/$(TW_BINARY) $(TW_DOWNLOAD_URL)
	@echo "Verifying checksum..."
	@cd bin && grep $(TW_BINARY) ../$(TW_CHECKSUM_FILE) | sha256sum -c - || \
		{ echo "Checksum verification failed! Deleting binary..."; rm -f bin/$(TW_BINARY); exit 1; }
	mv bin/$(TW_BINARY) $@
	chmod +x $@

makefiles/tailwind-sha256sums.txt:
	@echo "Downloading checksum file..."
	curl --retry 5 -sLo $@ $(TW_CHECKSUM_URL)

TW_CSS_TARGET := static/build.css

src/main.css: tailwind
	@echo "Generating Tailwind CSS file..."
	bin/tailwindcss -i $@ -o $(TW_CSS_TARGET) --minify

tw-watch: tailwind
	@echo "Watching Tailwind"
	@bin/tailwindcss -i src/main.css -o $(TW_CSS_TARGET) -w

tw-clean:
	rm -f bin/tailwindcss static/tailwind.css

.PHONY: tailwind tw-clean
