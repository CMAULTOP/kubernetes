//! Authentication primitives that establish typed Kubernetes request identities.
//!
//! This crate deliberately owns no HTTP transport and has no authorization policy. It maps a
//! presented credential to immutable identity attributes for the API Server and future RBAC layer.

use std::collections::{BTreeMap, BTreeSet};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use rsa::{
    pkcs8::{DecodePrivateKey, DecodePublicKey},
    traits::PublicKeyParts,
    RsaPrivateKey, RsaPublicKey,
};
use rusternetes_common::ApiError;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

/// Kubernetes request identity established before authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    pub username: String,
    pub uid: Option<String>,
    pub groups: BTreeSet<String>,
    pub extra: BTreeMap<String, Vec<String>>,
}

impl RequestIdentity {
    /// Creates an authenticated identity and adds Kubernetes' required authenticated group.
    pub fn authenticated(
        username: impl Into<String>,
        uid: Option<String>,
        groups: impl IntoIterator<Item = String>,
        extra: BTreeMap<String, Vec<String>>,
    ) -> Result<Self, ApiError> {
        let username = username.into();
        if username.is_empty() {
            return Err(ApiError::Invalid {
                message: "authenticated username must not be empty".to_owned(),
            });
        }
        let mut groups = groups.into_iter().collect::<BTreeSet<_>>();
        groups.insert("system:authenticated".to_owned());
        Ok(Self {
            username,
            uid,
            groups,
            extra,
        })
    }

    /// Kubernetes' configured anonymous identity.
    pub fn anonymous() -> Self {
        Self {
            username: "system:anonymous".to_owned(),
            uid: None,
            groups: ["system:unauthenticated".to_owned()].into_iter().collect(),
            extra: BTreeMap::new(),
        }
    }
}

/// Policy applied only when an HTTP request presents no authorization credential.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AnonymousPolicy {
    /// Preserve the standard Kubernetes anonymous identity for requests with no credentials.
    #[default]
    Allow,
    /// Require an authenticator to establish an identity for every request.
    Deny,
}

/// A configured public verification key for ServiceAccount JWTs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceAccountJwtKey {
    pub key_id: Option<String>,
    pub rsa_public_key_pem: String,
}

/// OIDC discovery document advertised for configured ServiceAccount signing keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceAccountOidcDiscoveryDocument {
    pub issuer: String,
    pub jwks_uri: String,
    pub response_types_supported: Vec<String>,
    pub subject_types_supported: Vec<String>,
    pub id_token_signing_alg_values_supported: Vec<String>,
}

/// JSON Web Key Set published for configured ServiceAccount verification keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceAccountJwksDocument {
    pub keys: Vec<ServiceAccountRsaJwk>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceAccountRsaJwk {
    pub kty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
    #[serde(rename = "use")]
    pub key_use: String,
    pub alg: String,
    pub n: String,
    pub e: String,
}

/// OIDC metadata and JWKS material derived from the exact configured verification keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceAccountOidcDiscovery {
    document: ServiceAccountOidcDiscoveryDocument,
    jwks: ServiceAccountJwksDocument,
}

impl ServiceAccountOidcDiscovery {
    pub fn new(
        issuer: impl Into<String>,
        jwks_uri: impl Into<String>,
        keys: &[ServiceAccountJwtKey],
    ) -> Result<Self, ApiError> {
        let issuer = issuer.into();
        let jwks_uri = jwks_uri.into();
        if !issuer.starts_with("https://") || !jwks_uri.starts_with("https://") || keys.is_empty() {
            return Err(ApiError::Invalid {
                message: "ServiceAccount OIDC issuer, JWKS URI and verification keys must be HTTPS and non-empty"
                    .to_owned(),
            });
        }
        let mut jwks = Vec::with_capacity(keys.len());
        for key in keys {
            let public_key =
                RsaPublicKey::from_public_key_pem(&key.rsa_public_key_pem).map_err(|_| {
                    ApiError::Invalid {
                        message: "configured ServiceAccount OIDC public key is invalid".to_owned(),
                    }
                })?;
            jwks.push(ServiceAccountRsaJwk {
                kty: "RSA".to_owned(),
                kid: key.key_id.clone(),
                key_use: "sig".to_owned(),
                alg: "RS256".to_owned(),
                n: URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be()),
                e: URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be()),
            });
        }
        Ok(Self {
            document: ServiceAccountOidcDiscoveryDocument {
                issuer,
                jwks_uri,
                response_types_supported: vec!["id_token".to_owned()],
                subject_types_supported: vec!["public".to_owned()],
                id_token_signing_alg_values_supported: vec!["RS256".to_owned()],
            },
            jwks: ServiceAccountJwksDocument { keys: jwks },
        })
    }

    pub fn document(&self) -> &ServiceAccountOidcDiscoveryDocument {
        &self.document
    }

    pub fn jwks(&self) -> &ServiceAccountJwksDocument {
        &self.jwks
    }
}

/// Cryptographically verified Kubernetes ServiceAccount JWT claims.
///
/// This typed result deliberately does not establish a request identity by itself: callers must
/// still verify the signed ServiceAccount UID against live storage before accepting the token.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VerifiedServiceAccountJwt {
    pub iss: String,
    pub sub: String,
    #[serde(default)]
    pub aud: Vec<String>,
    #[serde(rename = "kubernetes.io")]
    pub kubernetes: KubernetesServiceAccountClaims,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KubernetesServiceAccountClaims {
    pub namespace: String,
    #[serde(rename = "serviceaccount")]
    pub service_account: KubernetesServiceAccountIdentityClaims,
    #[serde(default)]
    pub pod: Option<KubernetesBoundObjectClaims>,
    #[serde(default)]
    pub node: Option<KubernetesBoundObjectClaims>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KubernetesServiceAccountIdentityClaims {
    pub name: String,
    pub uid: String,
}

/// Name and UID claim for a Pod- or Node-bound ServiceAccount token.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KubernetesBoundObjectClaims {
    pub name: String,
    pub uid: String,
}

/// Verifies ServiceAccount JWS signatures and standard issuer, audience and time claims.
///
/// The verifier accepts only RS256 tokens, selects configured verification keys by `kid`, and
/// returns no identity until a caller has performed live object binding checks.
#[derive(Clone, Debug)]
pub struct ServiceAccountJwtVerifier {
    issuer: String,
    audiences: Vec<String>,
    keys: Vec<ServiceAccountJwtKey>,
}

impl ServiceAccountJwtVerifier {
    pub fn new(
        issuer: impl Into<String>,
        audiences: Vec<String>,
        keys: Vec<ServiceAccountJwtKey>,
    ) -> Result<Self, ApiError> {
        let issuer = issuer.into();
        if issuer.is_empty() || audiences.is_empty() || keys.is_empty() {
            return Err(ApiError::Invalid {
                message: "ServiceAccount JWT issuer, audiences and verification keys are required"
                    .to_owned(),
            });
        }
        if keys.iter().any(|key| key.rsa_public_key_pem.is_empty()) {
            return Err(ApiError::Invalid {
                message: "ServiceAccount JWT verification keys must not be empty".to_owned(),
            });
        }
        Ok(Self {
            issuer,
            audiences,
            keys,
        })
    }

    pub fn oidc_discovery(
        &self,
        issuer: impl Into<String>,
        jwks_uri: impl Into<String>,
    ) -> Result<ServiceAccountOidcDiscovery, ApiError> {
        ServiceAccountOidcDiscovery::new(issuer, jwks_uri, &self.keys)
    }

    fn accepts_issuer(&self, issuer: &ServiceAccountTokenIssuer) -> Result<(), ApiError> {
        if self.issuer != issuer.issuer || self.audiences != issuer.default_audiences {
            return Err(ApiError::Invalid {
                message:
                    "ServiceAccount token issuer must use the verifier issuer and default audiences"
                        .to_owned(),
            });
        }
        let signing_public_key = issuer.public_key()?;
        let matches = self.keys.iter().any(|configured| {
            configured.key_id.as_deref() == Some(issuer.key_id.as_str())
                && RsaPublicKey::from_public_key_pem(&configured.rsa_public_key_pem).is_ok_and(
                    |public_key| {
                        public_key.n() == signing_public_key.n()
                            && public_key.e() == signing_public_key.e()
                    },
                )
        });
        if !matches {
            return Err(ApiError::Invalid {
                message: "ServiceAccount token issuer private key does not match a configured verifier key ID"
                    .to_owned(),
            });
        }
        Ok(())
    }

    pub fn verify(&self, token: &str) -> Result<VerifiedServiceAccountJwt, ApiError> {
        self.verify_for_audiences(token, &self.audiences)
    }

    /// Verifies one ServiceAccount JWT against an explicit non-empty resource-server audience
    /// set. TokenReview uses this to require audience intersection without reimplementing RS256,
    /// issuer, time or Kubernetes identity claim validation.
    pub fn verify_for_audiences(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<VerifiedServiceAccountJwt, ApiError> {
        if audiences.is_empty() || audiences.iter().any(String::is_empty) {
            return Err(ApiError::Invalid {
                message: "ServiceAccount JWT verification audiences must be non-empty".to_owned(),
            });
        }
        let header =
            decode_header(token).map_err(|_| unauthorized("invalid ServiceAccount JWT"))?;
        if header.alg != Algorithm::RS256 {
            return Err(unauthorized("ServiceAccount JWT must use RS256"));
        }
        let candidates = self
            .keys
            .iter()
            .filter(|key| match (&header.kid, &key.key_id) {
                (Some(requested), Some(configured)) => requested == configured,
                (None, _) => true,
                _ => false,
            });
        for key in candidates {
            let decoding_key = DecodingKey::from_rsa_pem(key.rsa_public_key_pem.as_bytes())
                .map_err(|_| ApiError::Invalid {
                    message: "configured ServiceAccount JWT public key is invalid".to_owned(),
                })?;
            let mut validation = Validation::new(Algorithm::RS256);
            validation.set_issuer(&[self.issuer.as_str()]);
            validation.set_audience(audiences);
            if let Ok(data) = decode::<VerifiedServiceAccountJwt>(token, &decoding_key, &validation)
            {
                return validate_service_account_claims(data.claims);
            }
        }
        Err(unauthorized("ServiceAccount JWT was not accepted"))
    }
}

/// Upper duration bound for one issued ServiceAccount TokenRequest credential.
pub const MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 31_536_000;

/// Signs time-bounded RS256 ServiceAccount TokenRequest JWTs.
///
/// A token issuer is accepted by `AuthenticationChain` only after its private key, issuer and
/// default audiences are matched against the active verifier configuration. This prevents issuing
/// credentials that the same control plane cannot authenticate.
#[derive(Clone, Debug)]
pub struct ServiceAccountTokenIssuer {
    issuer: String,
    default_audiences: Vec<String>,
    rsa_private_key_pem: String,
    key_id: String,
    default_expiration_seconds: i64,
    max_expiration_seconds: i64,
}

/// Immutable information that is signed into one ServiceAccount token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceAccountTokenSubject {
    pub namespace: String,
    pub service_account_name: String,
    pub service_account_uid: String,
    pub pod: Option<KubernetesBoundObjectClaims>,
    pub node: Option<KubernetesBoundObjectClaims>,
}

/// A signed bearer token and its exact expiration instant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IssuedServiceAccountToken {
    pub token: String,
    pub expiration_timestamp: OffsetDateTime,
}

#[derive(Serialize)]
struct SignedServiceAccountJwt {
    iss: String,
    sub: String,
    aud: Vec<String>,
    exp: i64,
    iat: i64,
    nbf: i64,
    jti: String,
    #[serde(rename = "kubernetes.io")]
    kubernetes: KubernetesServiceAccountClaims,
}

impl ServiceAccountTokenIssuer {
    pub fn new(
        issuer: impl Into<String>,
        default_audiences: Vec<String>,
        rsa_private_key_pem: impl Into<String>,
        key_id: impl Into<String>,
        default_expiration_seconds: i64,
        max_expiration_seconds: i64,
    ) -> Result<Self, ApiError> {
        let issuer = issuer.into();
        let rsa_private_key_pem = rsa_private_key_pem.into();
        let key_id = key_id.into();
        if issuer.is_empty()
            || default_audiences.is_empty()
            || default_audiences.iter().any(String::is_empty)
            || key_id.is_empty()
            || default_expiration_seconds <= 0
            || max_expiration_seconds < default_expiration_seconds
            || max_expiration_seconds > MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
        {
            return Err(ApiError::Invalid {
                message: "ServiceAccount token issuer requires issuer, non-empty default audiences, key ID, and bounded positive expiration settings".to_owned(),
            });
        }
        RsaPrivateKey::from_pkcs8_pem(&rsa_private_key_pem).map_err(|_| ApiError::Invalid {
            message: "configured ServiceAccount token signing private key is invalid PKCS#8 PEM"
                .to_owned(),
        })?;
        Ok(Self {
            issuer,
            default_audiences,
            rsa_private_key_pem,
            key_id,
            default_expiration_seconds,
            max_expiration_seconds,
        })
    }

    fn public_key(&self) -> Result<RsaPublicKey, ApiError> {
        RsaPrivateKey::from_pkcs8_pem(&self.rsa_private_key_pem)
            .map(RsaPublicKey::from)
            .map_err(|_| ApiError::Invalid {
                message:
                    "configured ServiceAccount token signing private key is invalid PKCS#8 PEM"
                        .to_owned(),
            })
    }

    pub fn issue(
        &self,
        subject: ServiceAccountTokenSubject,
        requested_audiences: &[String],
        requested_expiration_seconds: Option<i64>,
        now: OffsetDateTime,
    ) -> Result<IssuedServiceAccountToken, ApiError> {
        if subject.namespace.is_empty()
            || subject.service_account_name.is_empty()
            || subject.service_account_uid.is_empty()
            || requested_audiences.iter().any(String::is_empty)
        {
            return Err(ApiError::Invalid {
                message: "ServiceAccount token subject and requested audiences must be non-empty"
                    .to_owned(),
            });
        }
        let requested_expiration_seconds =
            requested_expiration_seconds.unwrap_or(self.default_expiration_seconds);
        if requested_expiration_seconds <= 0 {
            return Err(ApiError::Invalid {
                message: "requested ServiceAccount token expiration must be greater than zero"
                    .to_owned(),
            });
        }
        let expiration_seconds = requested_expiration_seconds.min(self.max_expiration_seconds);
        let expiration_timestamp = now
            .checked_add(Duration::seconds(expiration_seconds))
            .ok_or(ApiError::Internal)?;
        let audiences = if requested_audiences.is_empty() {
            self.default_audiences.clone()
        } else {
            requested_audiences.to_vec()
        };
        let claims = SignedServiceAccountJwt {
            iss: self.issuer.clone(),
            sub: format!(
                "system:serviceaccount:{}:{}",
                subject.namespace, subject.service_account_name
            ),
            aud: audiences,
            exp: expiration_timestamp.unix_timestamp(),
            iat: now.unix_timestamp(),
            nbf: now.unix_timestamp(),
            jti: Uuid::new_v4().to_string(),
            kubernetes: KubernetesServiceAccountClaims {
                namespace: subject.namespace,
                service_account: KubernetesServiceAccountIdentityClaims {
                    name: subject.service_account_name,
                    uid: subject.service_account_uid,
                },
                pod: subject.pod,
                node: subject.node,
            },
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.key_id.clone());
        let signing_key = EncodingKey::from_rsa_pem(self.rsa_private_key_pem.as_bytes())
            .map_err(|_| ApiError::Internal)?;
        let token = encode(&header, &claims, &signing_key).map_err(|_| ApiError::Internal)?;
        Ok(IssuedServiceAccountToken {
            token,
            expiration_timestamp,
        })
    }
}

fn validate_service_account_claims(
    claims: VerifiedServiceAccountJwt,
) -> Result<VerifiedServiceAccountJwt, ApiError> {
    let expected_subject = format!(
        "system:serviceaccount:{}:{}",
        claims.kubernetes.namespace, claims.kubernetes.service_account.name
    );
    if claims.sub != expected_subject
        || claims.kubernetes.service_account.uid.is_empty()
        || claims
            .kubernetes
            .pod
            .as_ref()
            .is_some_and(|pod| pod.name.is_empty() || pod.uid.is_empty())
        || claims
            .kubernetes
            .node
            .as_ref()
            .is_some_and(|node| node.name.is_empty() || node.uid.is_empty())
    {
        return Err(unauthorized(
            "ServiceAccount JWT identity claims are inconsistent",
        ));
    }
    Ok(claims)
}

/// A development / bootstrap bearer token mapping. It is not a ServiceAccount or OIDC token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticBearerToken {
    token: String,
    identity: RequestIdentity,
}

impl StaticBearerToken {
    pub fn new(token: impl Into<String>, identity: RequestIdentity) -> Result<Self, ApiError> {
        let token = token.into();
        if token.is_empty() {
            return Err(ApiError::Invalid {
                message: "static bearer token must not be empty".to_owned(),
            });
        }
        Ok(Self { token, identity })
    }
}

/// A deterministic authentication chain with the initial static bearer authenticator.
///
/// Future credential modules, such as ServiceAccount JWT and X.509 authentication, join this
/// boundary rather than coupling protocol parsing to individual API resource handlers.
#[derive(Clone, Debug, Default)]
pub struct AuthenticationChain {
    anonymous_policy: AnonymousPolicy,
    static_bearer_tokens: Vec<StaticBearerToken>,
    service_account_jwt_verifier: Option<ServiceAccountJwtVerifier>,
    service_account_token_issuer: Option<ServiceAccountTokenIssuer>,
    service_account_oidc_discovery: Option<ServiceAccountOidcDiscovery>,
}

impl AuthenticationChain {
    pub fn new(
        anonymous_policy: AnonymousPolicy,
        static_bearer_tokens: Vec<StaticBearerToken>,
    ) -> Self {
        Self {
            anonymous_policy,
            static_bearer_tokens,
            service_account_jwt_verifier: None,
            service_account_token_issuer: None,
            service_account_oidc_discovery: None,
        }
    }

    /// Adds the cryptographic ServiceAccount token verifier. The API Server must still bind
    /// verified claims to live storage before accepting a token as an authenticated identity.
    pub fn with_service_account_jwt_verifier(
        mut self,
        verifier: ServiceAccountJwtVerifier,
    ) -> Self {
        self.service_account_jwt_verifier = Some(verifier);
        self
    }

    pub fn service_account_jwt_verifier(&self) -> Option<&ServiceAccountJwtVerifier> {
        self.service_account_jwt_verifier.as_ref()
    }

    /// Enables token issuance only when the signing private key matches the active verifier's
    /// public key (including key ID), issuer and default audience configuration.
    pub fn with_service_account_token_issuer(
        mut self,
        issuer: ServiceAccountTokenIssuer,
    ) -> Result<Self, ApiError> {
        let verifier =
            self.service_account_jwt_verifier
                .as_ref()
                .ok_or_else(|| ApiError::Invalid {
                    message: "ServiceAccount JWT verifier must be configured before token issuance"
                        .to_owned(),
                })?;
        verifier.accepts_issuer(&issuer)?;
        self.service_account_token_issuer = Some(issuer);
        Ok(self)
    }

    pub fn service_account_token_issuer(&self) -> Option<&ServiceAccountTokenIssuer> {
        self.service_account_token_issuer.as_ref()
    }

    /// Enables OIDC discovery derived from the same configured ServiceAccount JWT verifier keys.
    pub fn with_service_account_oidc_discovery(
        mut self,
        issuer: impl Into<String>,
        jwks_uri: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let verifier =
            self.service_account_jwt_verifier
                .as_ref()
                .ok_or_else(|| ApiError::Invalid {
                    message: "ServiceAccount JWT verifier must be configured before OIDC discovery"
                        .to_owned(),
                })?;
        self.service_account_oidc_discovery = Some(verifier.oidc_discovery(issuer, jwks_uri)?);
        Ok(self)
    }

    pub fn service_account_oidc_discovery(&self) -> Option<&ServiceAccountOidcDiscovery> {
        self.service_account_oidc_discovery.as_ref()
    }

    /// Verifies an explicitly presented ServiceAccount JWT when this chain has been configured
    /// with a verifier. The returned claims are not yet an authenticated identity.
    pub fn verify_service_account_jwt(
        &self,
        authorization: Option<&str>,
    ) -> Result<Option<VerifiedServiceAccountJwt>, ApiError> {
        let Some(authorization) = authorization else {
            return Ok(None);
        };
        let Some(verifier) = self.service_account_jwt_verifier() else {
            return Ok(None);
        };
        verifier
            .verify(parse_bearer_token(authorization)?)
            .map(Some)
    }

    /// Authenticates an optional RFC 6750-style Authorization value.
    ///
    /// An invalid supplied credential never degrades into an anonymous request. Static token
    /// comparison uses `subtle`'s constant-time primitive; token material is not included in errors.
    pub fn authenticate(&self, authorization: Option<&str>) -> Result<RequestIdentity, ApiError> {
        let Some(authorization) = authorization else {
            return self.authenticate_missing_credential();
        };
        let token = parse_bearer_token(authorization)?;
        for configured in &self.static_bearer_tokens {
            if configured.token.as_bytes().ct_eq(token.as_bytes()).into() {
                return Ok(configured.identity.clone());
            }
        }
        Err(unauthorized("bearer credential was not accepted"))
    }

    fn authenticate_missing_credential(&self) -> Result<RequestIdentity, ApiError> {
        match self.anonymous_policy {
            AnonymousPolicy::Allow => Ok(RequestIdentity::anonymous()),
            AnonymousPolicy::Deny => Err(unauthorized("authentication credentials are required")),
        }
    }
}

fn parse_bearer_token(authorization: &str) -> Result<&str, ApiError> {
    let Some((scheme, token)) = authorization.split_once(' ') else {
        return Err(unauthorized("Authorization must use the Bearer scheme"));
    };
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.contains(char::is_whitespace)
    {
        return Err(unauthorized(
            "Authorization must contain one non-empty Bearer token",
        ));
    }
    Ok(token)
}

fn unauthorized(message: impl Into<String>) -> ApiError {
    ApiError::Unauthorized {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_account_jwt_verifier_requires_complete_configuration_and_rejects_malformed_tokens() {
        assert!(ServiceAccountJwtVerifier::new("", vec!["api".to_owned()], vec![]).is_err());
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("active".to_owned()),
                rsa_public_key_pem: "not a key".to_owned(),
            }],
        )
        .expect("non-empty configuration is structurally valid");
        assert!(matches!(
            verifier.verify("not-a-jwt"),
            Err(ApiError::Unauthorized { .. })
        ));
    }

    #[test]
    fn oidc_discovery_requires_verifier_and_publishes_configured_rsa_key() {
        const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAht2MPPSji0Vrbgk/gCyZ\nDLAfFNHUx7R697SBlj2meld3M7DUf5IVa4C9BxyTf2uhb35JNPAW0TYgQ9bg4n4/\n5DD8dFz1soqSHumaMO7a969VwHtJO5cPYAkKqXiGSxwkTQiF4MSmaoCvPlMkYF0/\n21stDUJkJcHr1VLsQfK5X660tdK9suWeW6zxYidwWCt94LalQ85lOcZjw3YfKymX\nnRrCWNPUme7dLFo2lBJ/K2wNuucUZXPGg50aeEgmr4OVTPxVApRL5b85taacmbGu\nXVy/oaUvF0M3iDkRgZNKN0vZPNvP4tc+KF/+DWDO1msmFYkiiG6848zqtRnY0DJ6\nyQIDAQAB\n-----END PUBLIC KEY-----\n";
        assert!(AuthenticationChain::default()
            .with_service_account_oidc_discovery(
                "https://issuer.example",
                "https://issuer.example/openid/v1/jwks"
            )
            .is_err());
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("active".to_owned()),
                rsa_public_key_pem: PUBLIC_KEY.to_owned(),
            }],
        )
        .expect("valid public verification key");
        let discovery = verifier
            .oidc_discovery(
                "https://issuer.example",
                "https://issuer.example/openid/v1/jwks",
            )
            .expect("OIDC document derives from verifier key");
        assert_eq!(discovery.document().issuer, "https://issuer.example");
        assert_eq!(discovery.jwks().keys.len(), 1);
        assert_eq!(discovery.jwks().keys[0].kid.as_deref(), Some("active"));
        assert_eq!(discovery.jwks().keys[0].kty, "RSA");
        assert!(!discovery.jwks().keys[0].n.is_empty());
        assert!(!discovery.jwks().keys[0].e.is_empty());
    }

    #[derive(serde::Serialize)]
    struct SignedServiceAccountClaims {
        iss: &'static str,
        sub: &'static str,
        aud: Vec<&'static str>,
        exp: usize,
        nbf: usize,
        #[serde(rename = "kubernetes.io")]
        kubernetes: SignedKubernetesClaims,
    }

    #[derive(serde::Serialize)]
    struct SignedKubernetesClaims {
        namespace: &'static str,
        serviceaccount: SignedServiceAccountIdentity,
    }

    #[derive(serde::Serialize)]
    struct SignedServiceAccountIdentity {
        name: &'static str,
        uid: &'static str,
    }

    #[test]
    fn service_account_jwt_verifier_accepts_valid_rs256_claims() {
        const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCG3Yw89KOLRWtu\nCT+ALJkMsB8U0dTHtHr3tIGWPaZ6V3czsNR/khVrgL0HHJN/a6Fvfkk08BbRNiBD\n1uDifj/kMPx0XPWyipIe6Zow7tr3r1XAe0k7lw9gCQqpeIZLHCRNCIXgxKZqgK8+\nUyRgXT/bWy0NQmQlwevVUuxB8rlfrrS10r2y5Z5brPFiJ3BYK33gtqVDzmU5xmPD\ndh8rKZedGsJY09SZ7t0sWjaUEn8rbA265xRlc8aDnRp4SCavg5VM/FUClEvlvzm1\nppyZsa5dXL+hpS8XQzeIORGBk0o3S9k828/i1z4oX/4NYM7WayYViSKIbrzjzOq1\nGdjQMnrJAgMBAAECggEAAw3JOyge++xafmdfNLvNy2fBjGsj8lG35xwDQy+qMWMB\no/4BEdJxAbosjZisDlqVkTy+06AMJDihime3N+m78KLbVJc2SRCyNlj70NfXxXwG\n6RDhm6PUCUyrHSNJhzHf8I6c2XYafpbjYPno/PWfmIv7/Szfr6swd+gkyWmBoRT/\nQFAyswqu7Zr0xqWaDpPvvpbnTxn0a/OdMbF/ttJnLEfK8RnO8RxKH4wkW8MUnQbp\nfOO1QQCTSTTy1lAfyB8Vpxtsa6qtXBeoyplPQk6xavZZE0CspSSCc35Cb3t0NT1L\nwmP3Djs+RT6nfjyqhH6B6Zkz1WFSJ1Ck/OzxqyRCAQKBgQC6J66SCJL1+lr904MT\nGXfHAh1iBnc9c/r3ei9D8IJYPUL6agrB6PghQTyTUUCXsuTkP7kZr4fDhl+YEmxV\nFsz4UjFwIu4/4GCPDyQxvK4e3nYrkeXcB1sSkqklQIGPexiNah7ZdOfSGYaMzZP+\nmyWZp9gHjfrTZMCPxTBl/yQIUQKBgQC5d3ck5W06nbeqI0cW1Mi0XRtZqiWOQf5J\njzHpqhKMMcodV4pF8JQxQEnQ0cCWuBeiRSa1ld14rSgcZrpWChLFUCuUTCN0bWNY\nJc1PVA1Er+7aKwvvYoLHzhuqZrHefzf0Pm4M0khZV9UsyGXehZ4lmKBMNvnblgbI\nYW7dwYsk+QKBgFpPOf+arUEsDcyqOiKf7l3bhsmxfVOQ2qYI3rlFCtcoEUBPBZ0B\nGq93aJ3Hg2CU5zpcN75gS6rtm565AVleUF3/8gAG0jKm9fExVUvTz10ma4nDpBHU\nd7hQ8kIiQziKbWTdoM26S2TAAWh5q1yPg/RBWyp/FLpNXKXi8hHpb1+hAoGALzQg\ntttNuaV6oWrpJP5zNrSbyW5ssJBLUB2J7pbCsbvaXS1ym+pnTUG3h9Za1gF0wnAn\nMgA6pgQsOU5MDqnxrRaCgPP/8hoFNuIoJxCVb+33NL/QAdVow8HJeM06aA6pBxj8\nmXbLwzF/qC44/zGy1o7J/Zvga+r7PvTNatNfvsECgYBD+cN52j+WJ8ZRYquvMM26\n2FCfDwxGPDayJcpG2iPq1qQtc+wPAFUM2OAnh99ISwiqpwV2kBY4651/z40QLy9q\nj4WA5AZceC4lk4woTcoA5Q+3ngH+9q2AoT7kqK70hXd434i6weLDIOuKbV8akv2K\nyERt1OwAWzIuLBxT/hKsug==\n-----END PRIVATE KEY-----\n";
        const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAht2MPPSji0Vrbgk/gCyZ\nDLAfFNHUx7R697SBlj2meld3M7DUf5IVa4C9BxyTf2uhb35JNPAW0TYgQ9bg4n4/\n5DD8dFz1soqSHumaMO7a969VwHtJO5cPYAkKqXiGSxwkTQiF4MSmaoCvPlMkYF0/\n21stDUJkJcHr1VLsQfK5X660tdK9suWeW6zxYidwWCt94LalQ85lOcZjw3YfKymX\nnRrCWNPUme7dLFo2lBJ/K2wNuucUZXPGg50aeEgmr4OVTPxVApRL5b85taacmbGu\nXVy/oaUvF0M3iDkRgZNKN0vZPNvP4tc+KF/+DWDO1msmFYkiiG6848zqtRnY0DJ6\nyQIDAQAB\n-----END PUBLIC KEY-----\n";
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some("active".to_owned());
        let token = jsonwebtoken::encode(
            &header,
            &SignedServiceAccountClaims {
                iss: "https://issuer.example",
                sub: "system:serviceaccount:default:build-robot",
                aud: vec!["api"],
                exp: 2_000_000_000,
                nbf: 1,
                kubernetes: SignedKubernetesClaims {
                    namespace: "default",
                    serviceaccount: SignedServiceAccountIdentity {
                        name: "build-robot",
                        uid: "uid-42",
                    },
                },
            },
            &jsonwebtoken::EncodingKey::from_rsa_pem(PRIVATE_KEY.as_bytes())
                .expect("test key parses"),
        )
        .expect("test JWT signs");
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("active".to_owned()),
                rsa_public_key_pem: PUBLIC_KEY.to_owned(),
            }],
        )
        .expect("configuration is valid");
        let claims = verifier.verify(&token).expect("signed token verifies");
        assert_eq!(claims.sub, "system:serviceaccount:default:build-robot");
        assert_eq!(claims.kubernetes.service_account.uid, "uid-42");
    }

    #[test]
    fn token_issuer_emits_verifier_compatible_rs256_token_with_capped_lifetime() {
        const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCG3Yw89KOLRWtu\nCT+ALJkMsB8U0dTHtHr3tIGWPaZ6V3czsNR/khVrgL0HHJN/a6Fvfkk08BbRNiBD\n1uDifj/kMPx0XPWyipIe6Zow7tr3r1XAe0k7lw9gCQqpeIZLHCRNCIXgxKZqgK8+\nUyRgXT/bWy0NQmQlwevVUuxB8rlfrrS10r2y5Z5brPFiJ3BYK33gtqVDzmU5xmPD\ndh8rKZedGsJY09SZ7t0sWjaUEn8rbA265xRlc8aDnRp4SCavg5VM/FUClEvlvzm1\nppyZsa5dXL+hpS8XQzeIORGBk0o3S9k828/i1z4oX/4NYM7WayYViSKIbrzjzOq1\nGdjQMnrJAgMBAAECggEAAw3JOyge++xafmdfNLvNy2fBjGsj8lG35xwDQy+qMWMB\no/4BEdJxAbosjZisDlqVkTy+06AMJDihime3N+m78KLbVJc2SRCyNlj70NfXxXwG\n6RDhm6PUCUyrHSNJhzHf8I6c2XYafpbjYPno/PWfmIv7/Szfr6swd+gkyWmBoRT/\nQFAyswqu7Zr0xqWaDpPvvpbnTxn0a/OdMbF/ttJnLEfK8RnO8RxKH4wkW8MUnQbp\nfOO1QQCTSTTy1lAfyB8Vpxtsa6qtXBeoyplPQk6xavZZE0CspSSCc35Cb3t0NT1L\nwmP3Djs+RT6nfjyqhH6B6Zkz1WFSJ1Ck/OzxqyRCAQKBgQC6J66SCJL1+lr904MT\nGXfHAh1iBnc9c/r3ei9D8IJYPUL6agrB6PghQTyTUUCXsuTkP7kZr4fDhl+YEmxV\nFsz4UjFwIu4/4GCPDyQxvK4e3nYrkeXcB1sSkqklQIGPexiNah7ZdOfSGYaMzZP+\nmyWZp9gHjfrTZMCPxTBl/yQIUQKBgQC5d3ck5W06nbeqI0cW1Mi0XRtZqiWOQf5J\njzHpqhKMMcodV4pF8JQxQEnQ0cCWuBeiRSa1ld14rSgcZrpWChLFUCuUTCN0bWNY\nJc1PVA1Er+7aKwvvYoLHzhuqZrHefzf0Pm4M0khZV9UsyGXehZ4lmKBMNvnblgbI\nYW7dwYsk+QKBgFpPOf+arUEsDcyqOiKf7l3bhsmxfVOQ2qYI3rlFCtcoEUBPBZ0B\nGq93aJ3Hg2CU5zpcN75gS6rtm565AVleUF3/8gAG0jKm9fExVUvTz10ma4nDpBHU\nd7hQ8kIiQziKbWTdoM26S2TAAWh5q1yPg/RBWyp/FLpNXKXi8hHpb1+hAoGALzQg\ntttNuaV6oWrpJP5zNrSbyW5ssJBLUB2J7pbCsbvaXS1ym+pnTUG3h9Za1gF0wnAn\nMgA6pgQsOU5MDqnxrRaCgPP/8hoFNuIoJxCVb+33NL/QAdVow8HJeM06aA6pBxj8\nmXbLwzF/qC44/zGy1o7J/Zvga+r7PvTNatNfvsECgYBD+cN52j+WJ8ZRYquvMM26\n2FCfDwxGPDayJcpG2iPq1qQtc+wPAFUM2OAnh99ISwiqpwV2kBY4651/z40QLy9q\nj4WA5AZceC4lk4woTcoA5Q+3ngH+9q2AoT7kqK70hXd434i6weLDIOuKbV8akv2K\nyERt1OwAWzIuLBxT/hKsug==\n-----END PRIVATE KEY-----\n";
        const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAht2MPPSji0Vrbgk/gCyZ\nDLAfFNHUx7R697SBlj2meld3M7DUf5IVa4C9BxyTf2uhb35JNPAW0TYgQ9bg4n4/\n5DD8dFz1soqSHumaMO7a969VwHtJO5cPYAkKqXiGSxwkTQiF4MSmaoCvPlMkYF0/\n21stDUJkJcHr1VLsQfK5X660tdK9suWeW6zxYidwWCt94LalQ85lOcZjw3YfKymX\nnRrCWNPUme7dLFo2lBJ/K2wNuucUZXPGg50aeEgmr4OVTPxVApRL5b85taacmbGu\nXVy/oaUvF0M3iDkRgZNKN0vZPNvP4tc+KF/+DWDO1msmFYkiiG6848zqtRnY0DJ6\nyQIDAQAB\n-----END PUBLIC KEY-----\n";
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("active".to_owned()),
                rsa_public_key_pem: PUBLIC_KEY.to_owned(),
            }],
        )
        .expect("valid verifier");
        let issuer = ServiceAccountTokenIssuer::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            PRIVATE_KEY,
            "active",
            10,
            20,
        )
        .expect("valid token issuer");
        let chain = AuthenticationChain::default()
            .with_service_account_jwt_verifier(verifier.clone())
            .with_service_account_token_issuer(issuer.clone())
            .expect("matching verifier and issuer configurations");
        assert!(chain.service_account_token_issuer().is_some());
        let issued = issuer
            .issue(
                ServiceAccountTokenSubject {
                    namespace: "default".to_owned(),
                    service_account_name: "build-robot".to_owned(),
                    service_account_uid: "sa-uid".to_owned(),
                    pod: Some(KubernetesBoundObjectClaims {
                        name: "workload".to_owned(),
                        uid: "pod-uid".to_owned(),
                    }),
                    node: None,
                },
                &[],
                Some(1_000),
                OffsetDateTime::now_utc(),
            )
            .expect("token signs");
        let claims = verifier.verify(&issued.token).expect("token verifies");
        assert_eq!(claims.aud, ["api"]);
        assert_eq!(claims.kubernetes.service_account.uid, "sa-uid");
        assert_eq!(claims.kubernetes.pod.expect("pod binding").uid, "pod-uid");
        assert_eq!(
            decode_header(&issued.token)
                .expect("JWT header parses")
                .kid
                .as_deref(),
            Some("active")
        );
        assert!(issued.expiration_timestamp <= OffsetDateTime::now_utc() + Duration::seconds(21));
    }

    fn chain(policy: AnonymousPolicy) -> AuthenticationChain {
        let identity = RequestIdentity::authenticated(
            "bootstrap-admin",
            Some("subject-42".to_owned()),
            ["system:bootstrappers".to_owned()],
            BTreeMap::new(),
        )
        .expect("valid identity");
        let token = StaticBearerToken::new("bootstrap-secret", identity).expect("valid token");
        AuthenticationChain::new(policy, vec![token])
    }

    #[test]
    fn static_bearer_identity_is_authenticated_and_grouped() {
        let identity = chain(AnonymousPolicy::Deny)
            .authenticate(Some("Bearer bootstrap-secret"))
            .expect("configured bearer token authenticates");
        assert_eq!(identity.username, "bootstrap-admin");
        assert!(identity.groups.contains("system:authenticated"));
        assert!(identity.groups.contains("system:bootstrappers"));
    }

    #[test]
    fn invalid_bearer_never_becomes_anonymous() {
        assert!(matches!(
            chain(AnonymousPolicy::Allow).authenticate(Some("Bearer wrong-secret")),
            Err(ApiError::Unauthorized { .. })
        ));
    }

    #[test]
    fn missing_credentials_follow_explicit_anonymous_policy() {
        assert_eq!(
            chain(AnonymousPolicy::Allow)
                .authenticate(None)
                .expect("anonymous policy allows absent credential")
                .username,
            "system:anonymous"
        );
        assert!(matches!(
            chain(AnonymousPolicy::Deny).authenticate(None),
            Err(ApiError::Unauthorized { .. })
        ));
    }
}
