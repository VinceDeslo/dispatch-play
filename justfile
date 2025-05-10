__default:
    just --list

fmt:
    alejandra .
    cargo fmt

run:
    cargo run --quiet
