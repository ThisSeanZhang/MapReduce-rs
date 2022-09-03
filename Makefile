PLUGIN=wc

build:
	cargo build --release

seq: build
	cargo run --release --package core-sequential -- -p plugin_${PLUGIN} data-raw/*

server:
	cargo run --release --package core-distributed --bin coordinator -- -n 10 data-raw/*

client:
	rm -rf data-processed/*
	touch data-processed/.gitkeep
	cargo run --release --package core-distributed --bin worker -- -p plugin_${PLUGIN}

