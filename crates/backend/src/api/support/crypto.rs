//! 加密与凭据哈希工具。

use agent_hub_shared::*;
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn opaque_secret(prefix: &str) -> String {
    format!(
        "{prefix}{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

pub(crate) fn password_hash(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .to_string())
}

pub(crate) fn verify_password(stored: &str, candidate: &str) -> bool {
    if stored.starts_with("$argon2") {
        return PasswordHash::new(stored).ok().is_some_and(|hash| {
            Argon2::default()
                .verify_password(candidate.as_bytes(), &hash)
                .is_ok()
        });
    }
    if let Some(rest) = stored.strip_prefix("sha256:") {
        let Some((salt, expected_hash)) = rest.split_once(':') else {
            return false;
        };
        let candidate_hash = sha256_hex(&format!("{salt}:{candidate}"));
        return constant_time_eq(expected_hash.as_bytes(), candidate_hash.as_bytes());
    }
    // 兼容最早开发库中的明文密码，成功登录后会立即升级为 Argon2id。
    constant_time_eq(stored.as_bytes(), candidate.as_bytes())
}

pub(crate) fn password_needs_upgrade(stored: &str) -> bool {
    !stored.starts_with("$argon2id$")
}

pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
