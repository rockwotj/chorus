#!/usr/bin/env bash
set -euo pipefail
# Keep the mirror override process-local; the regional mirror stalled during
# the recorded run. No changes to /etc/apt or project-wide configuration.
apt_options=(-o "Dir::Etc::sourcelist=$PWD/test-ubuntu.sources" -o Dir::Etc::sourceparts=-
    -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30)
sudo apt-get "${apt_options[@]}" update -qq
sudo apt-get "${apt_options[@]}" install -y -qq build-essential pkg-config libssl-dev curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.95.0
mkdir -p chorus-live
tar -xzf source.tar.gz -C chorus-live
cd chorus-live/bench/slatedb-live
"$HOME/.cargo/bin/cargo" build --release --locked
"$HOME/.cargo/bin/rustc" --version
sha256sum ../../../source.tar.gz target/release/chorus-slatedb-live src/main.rs Cargo.lock
