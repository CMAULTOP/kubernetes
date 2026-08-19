use std::{env, fs, io};

use rusternetes_api_server::{core_backend_from_etcd_config, router_with_core_backend_and_auth};
use rusternetes_authn::{
    AuthenticationChain, ServiceAccountJwtKey, ServiceAccountJwtVerifier, ServiceAccountTokenIssuer,
};
use tokio::net::TcpListener;

const DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 3_600;
const DEFAULT_SERVICE_ACCOUNT_MAX_TOKEN_EXPIRATION_SECONDS: i64 = 86_400;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_address =
        env::var("RUSTERNETES_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let backend = core_backend_from_etcd_config(
        env::var("RUSTERNETES_ETCD_ENDPOINTS").ok().as_deref(),
        env::var("RUSTERNETES_ETCD_PREFIX").ok().as_deref(),
    )
    .await
    .map_err(io::Error::other)?;
    let authentication = service_account_authentication_from_env()?;
    let listener = TcpListener::bind(&bind_address).await?;
    axum::serve(
        listener,
        router_with_core_backend_and_auth(backend, authentication),
    )
    .await
}

/// Loads an optional ServiceAccount TokenRequest signer/verifier configuration from files.
///
/// All three required values must be present together. Empty or partial configuration aborts
/// startup rather than exposing a route that can issue credentials without live verification.
fn service_account_authentication_from_env() -> io::Result<AuthenticationChain> {
    let issuer = env::var("RUSTERNETES_SERVICE_ACCOUNT_ISSUER").ok();
    let signing_key_file = env::var("RUSTERNETES_SERVICE_ACCOUNT_SIGNING_KEY_FILE").ok();
    let verification_key_file = env::var("RUSTERNETES_SERVICE_ACCOUNT_VERIFICATION_KEY_FILE").ok();
    let configured = [
        issuer.as_ref(),
        signing_key_file.as_ref(),
        verification_key_file.as_ref(),
    ];
    if configured.iter().all(Option::is_none) {
        return Ok(AuthenticationChain::default());
    }
    if configured.iter().any(Option::is_none) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RUSTERNETES_SERVICE_ACCOUNT_ISSUER, RUSTERNETES_SERVICE_ACCOUNT_SIGNING_KEY_FILE, and RUSTERNETES_SERVICE_ACCOUNT_VERIFICATION_KEY_FILE must be configured together",
        ));
    }
    let issuer = issuer.expect("checked above");
    let signing_key = fs::read_to_string(signing_key_file.expect("checked above"))?;
    let verification_key = fs::read_to_string(verification_key_file.expect("checked above"))?;
    let audiences = env::var("RUSTERNETES_SERVICE_ACCOUNT_AUDIENCES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|audience| !audience.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![issuer.clone()]);
    let key_id = env::var("RUSTERNETES_SERVICE_ACCOUNT_KEY_ID")
        .unwrap_or_else(|_| "service-account-0".to_owned());
    let default_expiration_seconds = parse_token_duration(
        "RUSTERNETES_SERVICE_ACCOUNT_TOKEN_DEFAULT_EXPIRATION_SECONDS",
        DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
    )?;
    let max_expiration_seconds = parse_token_duration(
        "RUSTERNETES_SERVICE_ACCOUNT_TOKEN_MAX_EXPIRATION_SECONDS",
        DEFAULT_SERVICE_ACCOUNT_MAX_TOKEN_EXPIRATION_SECONDS,
    )?;
    let verifier = ServiceAccountJwtVerifier::new(
        issuer.clone(),
        audiences.clone(),
        vec![ServiceAccountJwtKey {
            key_id: Some(key_id.clone()),
            rsa_public_key_pem: verification_key,
        }],
    )
    .map_err(io::Error::other)?;
    let token_issuer = ServiceAccountTokenIssuer::new(
        issuer,
        audiences,
        signing_key,
        key_id,
        default_expiration_seconds,
        max_expiration_seconds,
    )
    .map_err(io::Error::other)?;
    AuthenticationChain::default()
        .with_service_account_jwt_verifier(verifier)
        .with_service_account_token_issuer(token_issuer)
        .map_err(io::Error::other)
}

fn parse_token_duration(variable: &str, default: i64) -> io::Result<i64> {
    let Some(value) = env::var(variable).ok() else {
        return Ok(default);
    };
    value.parse::<i64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{variable} must be a signed integer number of seconds"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::parse_token_duration;

    #[test]
    fn default_token_duration_is_used_when_variable_is_absent() {
        let variable = "RUSTERNETES_TEST_MISSING_TOKEN_DURATION";
        std::env::remove_var(variable);
        assert_eq!(parse_token_duration(variable, 3600).unwrap(), 3600);
    }
}
