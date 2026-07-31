lint: && fmt
    @echo "Linting code..."
    cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings -W clippy::all -W clippy::pedantic -W clippy::nursery
    cargo dylint --all
    cargo deny check --hide-inclusion-graph
    @echo "Linting complete."

fmt:
    @echo "Formatting code..."
    cargo fmt --all
    @echo "Formatting complete."

lint-proto:
    @echo "Linting protobufs..."
    api-linter --set-exit-status -I proto -I proto/third_party/googleapis proto/revent/v1/*.proto
    @echo "Protobuf linting complete."

test:
    @echo "Running tests..."
    cargo test --all
    @echo "Testing complete."

tools:
    @echo  "Installing tools..."
    cargo install --locked prek
    cargo install cargo-dylint dylint-link
    cargo install --locked cargo-deny
    go install github.com/googleapis/api-linter/v2/cmd/api-linter@latest
    go install github.com/google/keep-sorted@v0.9.1
    @echo "Tools installation complete."
