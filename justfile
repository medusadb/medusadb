alias b := build
alias t := test
alias c := check
alias d := doc
alias be := bench

export RUSTDOCFLAGS := "-D warnings"

build:
  cargo build

test-setup:
  cargo install cargo-nextest

test:
  cargo nextest run
  cargo test --doc

check-setup:
  cargo install cargo-deny cargo-audit cargo-vet

check:
  cargo fmt --all -- --check
  cargo clippy --all --all-targets --all-features -- -D warnings
  cargo deny check
  cargo audit
  cargo vet

doc:
  cargo doc --workspace --no-deps

bench:
  cargo bench

dev-setup: check-setup test-setup
  cargo install cargo-insta cargo-lambda cargo-update

ci: check build test doc

clean:
  cargo clean
