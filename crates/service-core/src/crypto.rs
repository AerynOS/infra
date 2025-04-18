//! Create, sign and verify data via an ED25519 keypair
use std::{fmt, path::Path};

use base64::Engine;
use derive_more::{Display, From};
use ed25519_dalek::{
    SECRET_KEY_LENGTH, Signature, Signer,
    pkcs8::{DecodePrivateKey, EncodePrivateKey},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An ED25519 private + public key
#[derive(Debug, Clone)]
pub struct KeyPair(ed25519_dalek::SigningKey);

impl KeyPair {
    /// Generate a new key pair
    pub fn generate() -> Self {
        let mut rand = rand::thread_rng();
        let key = ed25519_dalek::SigningKey::generate(&mut rand);
        Self(key)
    }

    /// Return the raw byte array of the underlying private key. A [`KeyPair`]
    /// can be restored from this using [`KeyPair::try_from_bytes`]
    pub fn to_bytes(&self) -> [u8; SECRET_KEY_LENGTH] {
        self.0.to_bytes()
    }

    /// Reconstruct a [`KeyPair`] from raw private key bytes, such as
    /// returned by [`KeyPair::to_bytes`].
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self(ed25519_dalek::SigningKey::from_bytes(bytes.try_into().map_err(
            |_| Error::InvalidPrivateKeyLength { actual: bytes.len() },
        )?)))
    }

    /// The public key half of this key pair
    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verifying_key())
    }

    /// Encode the private key as PKCS8 DER format
    pub fn der(&self) -> Result<ed25519_dalek::pkcs8::SecretDocument, Error> {
        self.0.to_pkcs8_der().map_err(Error::EncodeDerPrivateKey)
    }

    /// Sign the provided message with this key pair
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.0.sign(message)
    }

    /// Load a PEM encoded PKCS8 private key from the provided path
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self(
            ed25519_dalek::SigningKey::read_pkcs8_pem_file(path).map_err(Error::LoadPemPrivateKey)?,
        ))
    }
}

/// Public key half of a [`KeyPair`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PublicKey(ed25519_dalek::VerifyingKey);

impl PublicKey {
    /// Encode the public key to a string
    pub fn encode(&self) -> EncodedPublicKey {
        EncodedPublicKey(base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(self.0.as_bytes()))
    }

    /// Verify a signature on a message with this keypair's public key
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), Error> {
        self.0.verify_strict(message, signature).map_err(Error::VerifySignature)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_bytes()[..]
    }
}

impl TryFrom<String> for PublicKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        EncodedPublicKey::decode(value.as_str())
    }
}

impl From<PublicKey> for String {
    fn from(value: PublicKey) -> Self {
        value.to_string()
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.encode().fmt(f)
    }
}

/// A string encoded [`PublicKey`]
#[derive(Debug, Clone, PartialEq, Eq, From, Display, Serialize, Deserialize)]
pub struct EncodedPublicKey(String);

impl EncodedPublicKey {
    /// Decode the [`EncodedPublicKey`]
    pub fn decoded(&self) -> Result<PublicKey, Error> {
        Self::decode(&self.0)
    }

    /// Decode the string as a [`PublicKey`]
    pub fn decode(key: &str) -> Result<PublicKey, Error> {
        let bytes = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(key)?
            .try_into()
            .unwrap_or_default();

        Ok(PublicKey(
            ed25519_dalek::VerifyingKey::from_bytes(&bytes).map_err(Error::DecodePublicKey)?,
        ))
    }
}

/// A string encoded [`Signature`]
#[derive(Debug, Clone, PartialEq, Eq, From, Display)]
pub struct EncodedSignature(String);

impl EncodedSignature {
    /// Decode the string as a [`Signature`]
    pub fn decode(signature: &str) -> Result<Signature, Error> {
        let bytes = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(signature)?
            .try_into()
            .unwrap_or([0; 64]);

        Ok(Signature::from_bytes(&bytes))
    }
}

/// A crypto error
#[derive(Debug, Error)]
pub enum Error {
    /// Base64 decoding failed
    #[error("base64 decode")]
    Base64Decode(#[from] base64::DecodeError),
    /// Public Key decoding failed
    #[error("decode public key")]
    DecodePublicKey(#[source] ed25519_dalek::SignatureError),
    /// Encoding public key as DER failed
    #[error("encode der public key")]
    EncodeDerPublicKey(#[from] ed25519_dalek::pkcs8::spki::Error),
    /// Encoding private key as DER failed
    #[error("encode der private key")]
    EncodeDerPrivateKey(#[source] ed25519_dalek::pkcs8::Error),
    /// Signature verification failed
    #[error("signature verification")]
    VerifySignature(#[source] ed25519_dalek::SignatureError),
    /// Loading pem encoded private key failed
    #[error("load pem private key")]
    LoadPemPrivateKey(#[source] ed25519_dalek::pkcs8::Error),
    /// Invalid private key length
    #[error("invalid private key length, expected {} got {actual}", SECRET_KEY_LENGTH)]
    InvalidPrivateKeyLength {
        /// Actual size
        actual: usize,
    },
}
