use std::fmt;

use aes_gcm::{
    aead::{Aead, Generate, KeyInit},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use zeroize::Zeroizing;

pub const MODEL_SECRET_KEY_ENV: &str = "HUB_MODEL_SECRET_KEY";
pub const MODEL_SECRET_NONCE_LENGTH: usize = 12;

#[derive(Clone)]
pub struct ModelSecretCipher {
    cipher: Aes256Gcm,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncryptedModelSecret {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

impl fmt::Debug for ModelSecretCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelSecretCipher")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ModelSecretError {
    #[error("HUB_MODEL_SECRET_KEY is required")]
    MissingKey,
    #[error("HUB_MODEL_SECRET_KEY must be valid base64")]
    InvalidKeyEncoding,
    #[error("HUB_MODEL_SECRET_KEY must decode to exactly 32 bytes")]
    InvalidKeyLength,
    #[error("model API key must not be empty")]
    EmptyApiKey,
    #[error("model secret encryption failed")]
    EncryptionFailed,
    #[error("model secret authentication failed")]
    DecryptionFailed,
    #[error("model secret nonce must be exactly 12 bytes")]
    InvalidNonceLength,
    #[error("decrypted model API key is not valid UTF-8")]
    InvalidPlaintext,
}

impl ModelSecretCipher {
    pub fn from_env() -> Result<Self, ModelSecretError> {
        match std::env::var(MODEL_SECRET_KEY_ENV) {
            Ok(encoded_key) => {
                let encoded_key = Zeroizing::new(encoded_key);
                Self::from_env_value(Some(encoded_key.as_str()))
            }
            Err(std::env::VarError::NotPresent) => Err(ModelSecretError::MissingKey),
            Err(std::env::VarError::NotUnicode(_)) => Err(ModelSecretError::InvalidKeyEncoding),
        }
    }

    pub fn from_env_value(encoded_key: Option<&str>) -> Result<Self, ModelSecretError> {
        let encoded_key = encoded_key.ok_or(ModelSecretError::MissingKey)?;
        let decoded_key = Zeroizing::new(
            STANDARD
                .decode(encoded_key)
                .map_err(|_| ModelSecretError::InvalidKeyEncoding)?,
        );
        if decoded_key.len() != 32 {
            return Err(ModelSecretError::InvalidKeyLength);
        }
        let cipher = Aes256Gcm::new_from_slice(&decoded_key)
            .map_err(|_| ModelSecretError::InvalidKeyLength)?;
        Ok(Self { cipher })
    }

    pub fn encrypt(&self, api_key: &str) -> Result<EncryptedModelSecret, ModelSecretError> {
        if api_key.is_empty() {
            return Err(ModelSecretError::EmptyApiKey);
        }
        let nonce = Nonce::generate();
        let ciphertext = self
            .cipher
            .encrypt(&nonce, api_key.as_bytes())
            .map_err(|_| ModelSecretError::EncryptionFailed)?;
        Ok(EncryptedModelSecret {
            ciphertext,
            nonce: nonce.to_vec(),
        })
    }

    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<String, ModelSecretError> {
        if nonce.len() != MODEL_SECRET_NONCE_LENGTH {
            return Err(ModelSecretError::InvalidNonceLength);
        }
        let nonce = Nonce::try_from(nonce).map_err(|_| ModelSecretError::InvalidNonceLength)?;
        let plaintext = self
            .cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| ModelSecretError::DecryptionFailed)?;
        String::from_utf8(plaintext).map_err(|_| ModelSecretError::InvalidPlaintext)
    }
}

#[cfg(test)]
mod model_secret_tests {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    use super::{ModelSecretCipher, ModelSecretError};

    fn model_secret_cipher() -> ModelSecretCipher {
        let encoded_key = STANDARD.encode([42_u8; 32]);
        ModelSecretCipher::from_env_value(Some(&encoded_key)).unwrap()
    }

    #[test]
    fn model_secret_rejects_missing_key() {
        let error = ModelSecretCipher::from_env_value(None).unwrap_err();

        assert_eq!(error, ModelSecretError::MissingKey);
    }

    #[test]
    fn model_secret_rejects_invalid_base64_key() {
        let error = ModelSecretCipher::from_env_value(Some("not base64!@")).unwrap_err();

        assert_eq!(error, ModelSecretError::InvalidKeyEncoding);
    }

    #[test]
    fn model_secret_rejects_wrong_length_key() {
        let encoded_key = STANDARD.encode([7_u8; 31]);
        let error = ModelSecretCipher::from_env_value(Some(&encoded_key)).unwrap_err();

        assert_eq!(error, ModelSecretError::InvalidKeyLength);
    }

    #[test]
    fn model_secret_round_trip() {
        let cipher = model_secret_cipher();
        let decryptor = cipher.clone();
        let encrypted = cipher.encrypt("provider-api-key").unwrap();

        let decrypted = decryptor
            .decrypt(&encrypted.ciphertext, &encrypted.nonce)
            .unwrap();

        assert_eq!(decrypted, "provider-api-key");
    }

    #[test]
    fn model_secret_rejects_empty_api_key() {
        let error = model_secret_cipher().encrypt("").unwrap_err();

        assert_eq!(error, ModelSecretError::EmptyApiKey);
    }

    #[test]
    fn model_secret_uses_distinct_nonce_and_ciphertext_for_same_plaintext() {
        let cipher = model_secret_cipher();

        let first = cipher.encrypt("same-provider-api-key").unwrap();
        let second = cipher.encrypt("same-provider-api-key").unwrap();

        assert_eq!(first.nonce.len(), super::MODEL_SECRET_NONCE_LENGTH);
        assert_eq!(second.nonce.len(), super::MODEL_SECRET_NONCE_LENGTH);
        assert_ne!(first.nonce, second.nonce);
        assert_ne!(first.ciphertext, second.ciphertext);
    }

    #[test]
    fn model_secret_rejects_tampered_ciphertext() {
        let cipher = model_secret_cipher();
        let mut encrypted = cipher.encrypt("provider-api-key").unwrap();
        encrypted.ciphertext[0] ^= 1;

        let error = cipher
            .decrypt(&encrypted.ciphertext, &encrypted.nonce)
            .unwrap_err();

        assert_eq!(error, ModelSecretError::DecryptionFailed);
    }
}
