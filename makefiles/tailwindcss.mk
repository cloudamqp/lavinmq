# Variables
TW_VERSION := 3.4.13
TW_BASE_URL := https://github.com/tailwindlabs/tailwindcss/releases/download/v$(TW_VERSION)
TW_CHECKSUM_FILE := sha256sums.txt

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
ifeq ($(ARCH), arm64)
	TW_ARCH := arm64
endif

# The target binary and checksum
TW_BINARY := tailwindcss-$(TW_OS)-$(TW_ARCH)
TW_DOWNLOAD_URL := $(TW_BASE_URL)/$(TW_BINARY)
TW_CHECKSUM_URL := $(TW_BASE_URL)/$(TW_CHECKSUM_FILE)

# Targets
tailwind: bin/tailwindcss

bin/tailwindcss: | bin
	@echo "Downloading Tailwind CSS binary for $(TW_OS)-$(TW_ARCH)..."
	curl -L -o bin/$(TW_BINARY) $(TW_DOWNLOAD_URL)
	$(MAKE) /tmp/$(TW_CHECKSUM_FILE)
	@echo "Verifying checksum..."
	@cd bin && grep $(TW_BINARY) /tmp/$(TW_CHECKSUM_FILE) | sha256sum -c - || \
		{ echo "Checksum verification failed! Deleting binary..."; rm -f bin/$(TW_BINARY); exit 1; }
	mv bin/$(TW_BINARY) $@
	chmod +x $@

/tmp/$(TW_CHECKSUM_FILE):
	@echo "Downloading checksum file..."
	curl -L -o $@ $(TW_CHECKSUM_URL)

TW_CSS_TARGET := static/build.css

src/main.css: tailwind
	@echo "Generating Tailwind CSS file..."
	bin/tailwindcss -i $@ -o $(TW_CSS_TARGET)

tw-watch: tailwind
	@echo "Watching Tailwind"
	@bin/tailwindcss -i src/main.css -o $(TW_CSS_TARGET) -w

tw-clean:
	rm -f bin/tailwindcss /tmp/$(TW_CHECKSUM_FILE) bin/$(TW_BINARY) static/tailwind.css

.PHONY: tailwind tw-clean
