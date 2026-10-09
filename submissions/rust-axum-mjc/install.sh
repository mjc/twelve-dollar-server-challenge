#!/usr/bin/env bash
# One-time toolchain installation on the challenge's Ubuntu 24.04 image.
set -euo pipefail
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
[[ "$EUID" == 0 ]] || { echo 'run install.sh as root' >&2; exit 1; }
apt-get update -qq
apt-get install -y -qq build-essential curl ca-certificates pkg-config xz-utils zlib1g-dev
mkdir -p "$HERE/.install"
cd "$HERE/.install"
export TMPDIR="$HERE/.install"
case "$(uname -m)" in
  x86_64) target=x86_64-unknown-linux-gnu ;;
  aarch64) target=aarch64-unknown-linux-gnu ;;
  *) echo 'unsupported architecture' >&2; exit 1 ;;
esac
if [[ ! -x /opt/rust-1.99.0/bin/rustc ]]; then
  archive="rust-1.99.0-$target.tar.xz"
  curl --fail --location --retry 3 -o "$archive" "https://static.rust-lang.org/dist/$archive"
  curl --fail --location --retry 3 -o "$archive.sha256" "https://static.rust-lang.org/dist/$archive.sha256"
  sha256sum --check "$archive.sha256"
  tar -xf "$archive"
  "rust-1.99.0-$target/install.sh" --prefix=/opt/rust-1.99.0 --without=rust-docs
fi
export PATH="/opt/rust-1.99.0/bin:$PATH"
export CARGO_HOME="$HERE/.install/cargo"
if [[ ! -x /opt/mold-3.0.0/bin/mold ]]; then
  curl --fail --location --retry 3 -o mold-3.0.0.tar.gz https://github.com/rui314/mold/archive/refs/tags/v3.0.0.tar.gz
  tar -xf mold-3.0.0.tar.gz
  cd mold-3.0.0
  cargo build --release --locked --jobs 1 -p mold-cli
  install -Dm755 target/release/mold /opt/mold-3.0.0/bin/mold
  install -Dm755 target/release/mold-wrapper.so /opt/mold-3.0.0/lib/mold/mold-wrapper.so
  ln -sf mold /opt/mold-3.0.0/bin/ld.mold
fi
