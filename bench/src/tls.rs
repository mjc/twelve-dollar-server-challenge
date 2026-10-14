use std::sync::OnceLock;

pub(super) fn initialize() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

fn config() -> &'static rustls::ClientConfig {
    initialize();
    static CONFIG: OnceLock<rustls::ClientConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .expect("TLS crypto provider initialized");
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("crypto provider supports enabled TLS versions")
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
            .with_no_client_auth();
        // Preconfigured reqwest TLS does not apply http1_only() to ALPN.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config
    })
}

pub(super) fn configure(
    builder: reqwest::ClientBuilder,
    accept_invalid_certs: bool,
) -> reqwest::ClientBuilder {
    if accept_invalid_certs {
        // Keep insecure resumption separate from verified connections.
        return builder.danger_accept_invalid_certs(true);
    }
    // Share roots/verifier and the bounded TLS session cache, not HTTP pools.
    builder.use_preconfigured_tls(config().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn config_clones_share_tls_state_and_advertise_only_http1() {
        initialize();
        let first = config().clone();
        let second = config().clone();
        assert!(Arc::ptr_eq(
            first.crypto_provider(),
            second.crypto_provider()
        ));
        assert!(Arc::ptr_eq(
            first.crypto_provider(),
            rustls::crypto::CryptoProvider::get_default().unwrap(),
        ));
        assert!(Arc::ptr_eq(
            &first.client_auth_cert_resolver,
            &second.client_auth_cert_resolver,
        ));
        assert_eq!(first.alpn_protocols, vec![b"http/1.1".to_vec()]);
        assert!(first.enable_sni);
    }
}
