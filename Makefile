PLUGIN=wc

build:
	cargo build --release

seq: build
	cargo run --release --package core-sequential -- -p plugin_${PLUGIN} data-raw/*