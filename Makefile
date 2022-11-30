test: loadable-dev
	python3 tests/test-loadable.py

loadable-dev:
	cargo build

format:
	cargo fmt

sqlite-xsv.h: cbindgen.toml
	rustup run nightly cbindgen  --config $< -o $@

.PHONY: test loadable-dev
