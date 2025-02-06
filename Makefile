SHELL := /bin/bash

VERSION=$(shell cat VERSION)

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
TARGET_LOADABLE_RELEASE=$(prefix)/release/xsv0.$(LOADABLE_EXTENSION)

TARGET_STATIC=$(prefix)/debug/xsv0.a
TARGET_STATIC_RELEASE=$(prefix)/release/xsv0.a

ifdef target
CARGO_TARGET=--target=$(target)
BUILT_LOCATION=target/$(target)/debug/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION)
BUILT_LOCATION_RELEASE=target/$(target)/release/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION)
else
CARGO_TARGET=
BUILT_LOCATION=target/debug/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION)
BUILT_LOCATION_RELEASE=target/release/$(LIBRARY_PREFIX)sqlite_xsv.$(LOADABLE_EXTENSION)
endif

ifdef python
PYTHON=$(python)
else
PYTHON=python3
endif


$(prefix):
	mkdir -p $(prefix)/debug
	mkdir -p $(prefix)/release

$(TARGET_LOADABLE): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build $(CARGO_TARGET)
	cp $(BUILT_LOCATION) $@

$(TARGET_LOADABLE_RELEASE): $(prefix) $(shell find . -type f -name '*.rs')
	cargo build --release $(CARGO_TARGET)
	cp $(BUILT_LOCATION_RELEASE) $@

Cargo.toml: VERSION
	cargo set-version `cat VERSION`


version: VERSION
	make Cargo.toml

format:
	cargo fmt

sqlite-xsv.h: cbindgen.toml
	rustup run nightly cbindgen  --config $< -o $@

release: $(TARGET_LOADABLE_RELEASE) $(TARGET_STATIC_RELEASE)

loadable: $(TARGET_LOADABLE)
loadable-release: $(TARGET_LOADABLE_RELEASE)

static: $(TARGET_STATIC)
static-release: $(TARGET_STATIC_RELEASE)

debug: loadable static
release: loadable-release static-release

clean:
	rm dist/*
	cargo clean

test-loadable:
	echo "UV_PYTHON_PREFERENCE=$(UV_PYTHON_PREFERENCE)"
	uv run tests/test-loadable.py

test:
	make test-loadable

publish-release:
	./scripts/publish_release.sh

.PHONY: clean \
	test test-loadable \
	loadable loadable-release \
	static static-release \
	debug release \
	format version publish-release
