test: loadable-dev
	python3 tests/test-loadable.py

loadable-dev:
	cargo build

format:
	cargo fmt

.PHONY: test loadable-dev