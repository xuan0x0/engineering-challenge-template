//! Kalshi API-key request signing over `timestamp_ms + method + path`,
//! base64 encoded. Kalshi issues two key types:
//! - RSA: RSA-PSS with MGF1-SHA256 and salt length = digest length.
//! - Ed25519: the message is signed directly.

use std::path::Path;

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::pkcs8::DecodePrivateKey as _;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::rand_core::OsRng;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};

enum SigningKey {
    Rsa(Box<BlindedSigningKey<Sha256>>),
    Ed25519(Box<ed25519_dalek::SigningKey>),
}

pub struct Signer {
    key_id: String,
    signing_key: SigningKey,
}

/// Headers required on an authenticated Kalshi request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthHeaders {
    pub key: String,
    pub signature: String,
    pub timestamp: String,
}

impl Signer {
    pub fn new(key_id: impl Into<String>, private_key: RsaPrivateKey) -> Self {
        Self {
            key_id: key_id.into(),
            signing_key: SigningKey::Rsa(Box::new(BlindedSigningKey::<Sha256>::new(private_key))),
        }
    }

    pub fn new_ed25519(key_id: impl Into<String>, private_key: ed25519_dalek::SigningKey) -> Self {
        Self {
            key_id: key_id.into(),
            signing_key: SigningKey::Ed25519(Box::new(private_key)),
        }
    }

    /// Loads a PEM private key: Ed25519 PKCS#8, or RSA in PKCS#1
    /// (`BEGIN RSA PRIVATE KEY`) or PKCS#8 (`BEGIN PRIVATE KEY`) form.
    pub fn from_pem_file(key_id: impl Into<String>, path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read private key {}", path.display()))?;
        Self::from_pem(key_id, &raw)
    }

    /// Accepts a PEM document, or a bare base64 PKCS#8 body without the
    /// BEGIN/END lines.
    pub fn from_pem(key_id: impl Into<String>, raw: &str) -> anyhow::Result<Self> {
        let pem = if raw.contains("-----BEGIN ") {
            normalize_pem(raw).context("malformed PEM private key")?
        } else {
            normalize_pem(&format!(
                "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
                raw.trim()
            ))
            .context("malformed private key")?
        };
        if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem) {
            return Ok(Self::new_ed25519(key_id, key));
        }
        let key = RsaPrivateKey::from_pkcs1_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(&pem))
            .context("private key is neither Ed25519 nor RSA PEM")?;
        Ok(Self::new(key_id, key))
    }

    pub fn headers(&self, timestamp_ms: i64, method: &str, path: &str) -> AuthHeaders {
        let message = format!("{timestamp_ms}{method}{path}");
        let signature = match &self.signing_key {
            SigningKey::Rsa(key) => key
                .sign_with_rng(&mut OsRng, message.as_bytes())
                .to_bytes()
                .to_vec(),
            SigningKey::Ed25519(key) => {
                use ed25519_dalek::Signer as _;
                key.sign(message.as_bytes()).to_bytes().to_vec()
            }
        };
        AuthHeaders {
            key: self.key_id.clone(),
            signature: BASE64.encode(signature),
            timestamp: timestamp_ms.to_string(),
        }
    }
}

/// Rebuilds a PEM document with canonical line breaks. Keys copied out of a web
/// page often arrive on one line (`-----BEGIN ...----- MC4C... -----END ...-----`),
/// which strict PEM parsers reject.
fn normalize_pem(raw: &str) -> Option<String> {
    const BEGIN: &str = "-----BEGIN ";
    let begin = raw.find(BEGIN)? + BEGIN.len();
    // Closing dashes of the BEGIN line, whether or not a newline follows.
    let label_end = begin + raw[begin..].find("-----")?;
    let label = raw[begin..label_end].trim();
    let body_start = label_end + 5;
    let end_marker = format!("-----END {label}-----");
    let body_end = body_start + raw[body_start..].find(&end_marker)?;
    let body: String = raw[body_start..body_end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let lines: Vec<&str> = body
        .as_bytes()
        .chunks(64)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or_default())
        .collect();
    Some(format!(
        "-----BEGIN {label}-----\n{}\n{end_marker}\n",
        lines.join("\n")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pss::{Signature, VerifyingKey};
    use rsa::signature::Verifier;

    #[test]
    fn ed25519_signature_verifies_and_one_line_pem_is_accepted() {
        // RFC 8410 example Ed25519 private key, flattened onto one line the way
        // the Kalshi UI displays it.
        let one_line = "-----BEGIN PRIVATE KEY----- \
            MC4CAQAwBQYDK2VwBCIEINTuctv5E1hK1bbY8fdp+K06/nwoy/HU++CXqI9EdVhC \
            -----END PRIVATE KEY-----";
        let signer = Signer::from_pem("key-id", one_line).unwrap();
        let headers = signer.headers(1_700_000_000_123, "GET", "/trade-api/ws/v2");

        let pem = normalize_pem(one_line).unwrap();
        let verifying = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem)
            .unwrap()
            .verifying_key();
        let raw: [u8; 64] = BASE64
            .decode(&headers.signature)
            .unwrap()
            .try_into()
            .unwrap();
        let sig = ed25519_dalek::Signature::from_bytes(&raw);
        verifying
            .verify_strict(b"1700000000123GET/trade-api/ws/v2", &sig)
            .expect("signature must verify");
        assert!(verifying.verify_strict(b"tampered", &sig).is_err());
    }

    #[test]
    fn normalizes_one_line_pem_with_trailing_newline() {
        let pem =
            normalize_pem("-----BEGIN PRIVATE KEY----- QUJD REVG -----END PRIVATE KEY-----\n");
        assert_eq!(
            pem.as_deref(),
            Some("-----BEGIN PRIVATE KEY-----\nQUJDREVG\n-----END PRIVATE KEY-----\n")
        );
    }

    #[test]
    fn accepts_bare_base64_body() {
        let bare = "MC4CAQAwBQYDK2VwBCIEINTuctv5E1hK1bbY8fdp+K06/nwoy/HU++CXqI9EdVhC\n";
        assert!(Signer::from_pem("key-id", bare).is_ok());
    }

    #[test]
    fn rejects_non_pem_input() {
        assert!(Signer::from_pem("k", "not a key").is_err());
    }

    #[test]
    fn signature_verifies_with_public_key() {
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let verifying = VerifyingKey::<Sha256>::new(private.to_public_key());
        let signer = Signer::new("key-id", private);

        let headers = signer.headers(1_700_000_000_123, "GET", "/trade-api/ws/v2");
        assert_eq!(headers.key, "key-id");
        assert_eq!(headers.timestamp, "1700000000123");

        let raw = BASE64.decode(&headers.signature).unwrap();
        let sig = Signature::try_from(raw.as_slice()).unwrap();
        verifying
            .verify(b"1700000000123GET/trade-api/ws/v2", &sig)
            .expect("signature must verify");
        assert!(verifying.verify(b"tampered", &sig).is_err());
    }
}
