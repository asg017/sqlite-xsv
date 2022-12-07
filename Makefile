
ifeq ($(shell uname -s),Darwin)
CONFIG_DARWIN=y
else ifeq ($(OS),Windows_NT)
CONFIG_WINDOWS=y
else
CONFIG_LINUX=y
endif

LIBRARY_PREFIX=lib
ifdef CONFIG_DARWIN
LOADABLE_EXTENSION=dylib
endif

ifdef CONFIG_LINUX
LOADABLE_EXTENSION=so
endif


ifdef CONFIG_WINDOWS
LOADABLE_EXTENSION=dll
LIBRARY_PREFIX=
endif

prefix=dist
TARGET_LOADABLE=$(prefix)/debug/xsv0.$(LOADABLE_EXTENSION)
TARGET_STATIC=$(prefix)/debug/xsv0.a

TARGET_LOADABLE_RELEASE=$(prefix)/release/xsv0.$(LOADABLE_EXTENSION)
TARGET_STATIC_RELEASE=$(prefix)/release/xsv0.a


$(prefix):
	mkdir -p $(prefix)/debug
	mkdir -p $(prefix)/release

$(TARGET_LOADABLE): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build 
	cp target/debug/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION) $@

$(TARGET_STATIC): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build 
	cp target/debug/$(LIBRARY_PREFIX)sqlite_xsv.a $@


$(TARGET_LOADABLE_RELEASE): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build --release 
	cp target/release/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION) $@

$(TARGET_STATIC_RELEASE): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build 
	cp target/debug/$(LIBRARY_PREFIX)sqlite_xsv.a $@

format:
	cargo fmt

sqlite-xsv.h: cbindgen.toml
	rustup run nightly cbindgen  --config $< -o $@

release: $(TARGET_LOADABLE_RELEASE) $(TARGET_STATIC_RELEASE)

loadable: $(TARGET_LOADABLE)
static: $(TARGET_STATIC)

clean:
	rm dist/*
	cargo clean

test:
	python3 tests/test-loadable.py

.PHONY: clean test loadable static