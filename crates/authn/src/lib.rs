//! Authentication primitives that establish typed Kubernetes request identities.
//!
//! This crate deliberately owns no HTTP transport and has no authorization policy. It maps a
//! presented credential to immutable identity attributes for the API Server and future RBAC layer.

use std::collections::{BTreeMap, BTreeSet};

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use rusternetes_common::ApiError;
use serde::Deserialize;
use subtle::ConstantTimeEq;

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

/// Cryptographically verified Kubernetes ServiceAccount JWT claims.
///
/// This typed result deliberately does not establish a request identity by itself: callers must
/// still verify the signed ServiceAccount UID against live storage before accepting the token.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct VerifiedServiceAccountJwt {
    pub iss: String,
    pub sub: String,
    #[serde(default)]
    pub aud: Vec<String>,
    #[serde(rename = "kubernetes.io")]
    pub kubernetes: KubernetesServiceAccountClaims,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct KubernetesServiceAccountClaims {
    pub namespace: String,
    #[serde(rename = "serviceaccount")]
    pub service_account: KubernetesServiceAccountIdentityClaims,
    #[serde(default)]
    pub pod: Option<KubernetesBoundObjectClaims>,
    #[serde(default)]
    pub node: Option<KubernetesBoundObjectClaims>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct KubernetesServiceAccountIdentityClaims {
    pub name: String,
    pub uid: String,
}

/// Name and UID claim for a Pod- or Node-bound ServiceAccount token.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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

    pub fn verify(&self, token: &str) -> Result<VerifiedServiceAccountJwt, ApiError> {
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
            validation.set_audience(&self.audiences);
            if let Ok(data) = decode::<VerifiedServiceAccountJwt>(token, &decoding_key, &validation)
            {
                return validate_service_account_claims(data.claims);
            }
        }
        Err(unauthorized("ServiceAccount JWT was not accepted"))
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
