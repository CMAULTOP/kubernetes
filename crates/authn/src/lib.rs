//! Authentication primitives that establish typed Kubernetes request identities.
//!
//! This crate deliberately owns no HTTP transport and has no authorization policy. It maps a
//! presented credential to immutable identity attributes for the API Server and future RBAC layer.

use std::collections::{BTreeMap, BTreeSet};

use rusternetes_common::ApiError;
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
}

impl AuthenticationChain {
    pub fn new(
        anonymous_policy: AnonymousPolicy,
        static_bearer_tokens: Vec<StaticBearerToken>,
    ) -> Self {
        Self {
            anonymous_policy,
            static_bearer_tokens,
        }
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
