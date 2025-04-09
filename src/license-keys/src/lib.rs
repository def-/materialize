// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};

#[cfg(feature = "signing")]
mod signing;
#[cfg(feature = "signing")]
pub use signing::{get_pubkey_pem, make_license_key};

const ISSUER: &str = "Materialize, Inc.";
// this will be used specifically by cloud to avoid needing to issue separate
// license keys for each environment when it comes up - just being able to
// share a single license key that allows all environments and never expires
// will be much simpler to maintain
const ANY_ENVIRONMENT_AUD: &str = "00000000-0000-0000-0000-000000000000";
// list of public keys which are allowed to validate license keys. this is a
// list to allow for key rotation if necessary.
const PUBLIC_KEYS: &[&str] = &[include_str!("license_keys/production.pub")];

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ExpirationBehavior {
    Warn,
    DisableClusterCreation,
    Disable,
}

#[derive(Debug, Clone, Copy)]
pub struct ValidatedLicenseKey {
    pub max_credit_consumption_rate: f64,
    pub allow_credit_consumption_override: bool,
    pub expiration_behavior: ExpirationBehavior,
    pub expired: bool,
}

impl ValidatedLicenseKey {
    pub fn for_tests() -> Self {
        Self {
            max_credit_consumption_rate: 999999.0,
            allow_credit_consumption_override: true,
            expiration_behavior: ExpirationBehavior::Warn,
            expired: false,
        }
    }

    pub fn max_credit_consumption_rate(&self) -> Option<f64> {
        if self.expired
            && matches!(
                self.expiration_behavior,
                ExpirationBehavior::DisableClusterCreation | ExpirationBehavior::Disable
            )
        {
            Some(0.0)
        } else if self.allow_credit_consumption_override {
            None
        } else {
            Some(self.max_credit_consumption_rate)
        }
    }
}

impl Default for ValidatedLicenseKey {
    fn default() -> Self {
        // this is used for the emulator if no license key is provided
        Self {
            max_credit_consumption_rate: 24.0,
            allow_credit_consumption_override: false,
            expiration_behavior: ExpirationBehavior::Disable,
            expired: false,
        }
    }
}

pub fn validate(license_key: &str, environment_id: &str) -> Result<ValidatedLicenseKey, Box<dyn std::error::Error>> {
    let mut err = "no public key found".to_string();

    for pubkey in PUBLIC_KEYS {
        match validate_with_pubkey(license_key, pubkey, environment_id) {
            Ok(key) => return Ok(key),
            Err(e) => err = e,
        }
    }

    Err(err.into())
}

fn validate_with_pubkey(
    license_key: &str,
    pubkey_pem: &str,
    environment_id: &str,
) -> Result<ValidatedLicenseKey, String> {
    let res = validate_with_pubkey_v1(license_key, pubkey_pem, environment_id);
    let err = match res {
        Ok(key) => return Ok(key),
        Err(e) => e,
    };

    let previous_versions: Vec<Box<dyn Fn() -> Result<ValidatedLicenseKey, String>>> = vec![
        // Add fallbacks here if needed
    ];

    for validator in previous_versions {
        if let Ok(key) = validator() {
            return Ok(key);
        }
    }

    Err(err)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    sub: String,
    exp: u64,
    nbf: u64,
    iss: String,
    aud: String,
    iat: u64,
    jti: String,

    version: u64,
    max_credit_consumption_rate: f64,
    #[serde(default, skip_serializing_if = "is_default")]
    allow_credit_consumption_override: bool,
    expiration_behavior: ExpirationBehavior,
}

fn validate_with_pubkey_v1(
    license_key: &str,
    pubkey_pem: &str,
    environment_id: &str,
) -> Result<ValidatedLicenseKey, String> {
    let mut validation = Validation::new(Algorithm::PS256);
    validation.set_required_spec_claims(&["exp", "nbf", "aud", "iss", "sub"]);
    validation.set_audience(&[environment_id, ANY_ENVIRONMENT_AUD]);
    validation.set_issuer(&[ISSUER]);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = true;

    let key = DecodingKey::from_rsa_pem(pubkey_pem.as_bytes())
        .map_err(|e| format!("invalid RSA key: {}", e))?;

    let (jwt, expired) = match jsonwebtoken::decode(license_key, &key, &validation) {
        Ok(jwt) => (jwt, false),
        Err(e) if matches!(e.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) => {
            validation.validate_exp = false;
            let jwt: TokenData<Payload> = jsonwebtoken::decode(license_key, &key, &validation)
                .map_err(|e| format!("failed to decode expired JWT: {}", e))?;
            (jwt, true)
        }
        Err(e) => return Err(format!("failed to decode JWT: {}", e)),
    };

    if jwt.header.typ.as_deref() != Some("JWT") {
        return Err("invalid jwt header type".to_string());
    }

    if jwt.claims.version != 1 {
        return Err("invalid license key version".to_string());
    }

    if !(jwt.claims.nbf..=jwt.claims.exp).contains(&jwt.claims.iat) {
        return Err("invalid issuance time".to_string());
    }

    Ok(ValidatedLicenseKey {
        max_credit_consumption_rate: jwt.claims.max_credit_consumption_rate,
        allow_credit_consumption_override: jwt.claims.allow_credit_consumption_override,
        expiration_behavior: jwt.claims.expiration_behavior,
        expired,
    })
}

fn is_default<T: PartialEq + Eq + Default>(val: &T) -> bool {
    *val == T::default()
}
