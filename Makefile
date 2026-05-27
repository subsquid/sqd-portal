.PHONY: openapi openapi-open client build-client

openapi: openapi.html

openapi.json:
	cargo run --bin gen_openapi > openapi.json

openapi.html: openapi.json
	npx @redocly/cli build-docs openapi.json --output openapi.html

openapi-open: openapi.html
	open openapi.html

client: openapi.json
	@command -v cargo-progenitor >/dev/null 2>&1 || cargo install progenitor-cli
	rustup run nightly cargo progenitor -i openapi.json -o ./generated/portal-client -n sqd-portal-client -v 0.1.0

build-client: client
	cargo build --manifest-path ./generated/portal-client/Cargo.toml
