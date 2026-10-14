# Local patch

Source: the published Goose 0.18.1 package from crates.io, upstream commit
`7d2a470a3f9051d1f4fdb72c841001c09324858e`. The original license and package
provenance are retained. The following published paths are omitted:

- `.cargo-ok` and `.cargo-checksum.json`: registry cache markers.
- `.github/`: upstream GitHub automation.
- `.clinerules`, `context7.json`, `memory-bank/`, and `sample_goosebot_*.md`:
  upstream assistant configuration and review samples, unused by Cargo.
- `src/docs/goose-book/`: the standalone mdBook site, including its Markdown,
  configuration, and image assets. Rust sources do not include these files;
  Cargo library, example, and test targets do not reference this directory.

All Rust modules, explicit example/test targets, manifests, README, and license
are retained.

`create_reqwest_client` calls `danger_accept_invalid_certs` only when Goose's
`default` or `rustls-tls` feature is enabled. Reqwest does not expose that method
without a TLS backend, so the unconditional call prevented this HTTP benchmark
from compiling without TLS. TLS-enabled builds preserve the existing certificate
validation setting. No other Goose behavior is changed.

Remove this patch when an upstream Goose release supports this build with TLS
disabled.
