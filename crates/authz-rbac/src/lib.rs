//! Typed, fail-closed Kubernetes RBAC authorization primitives.
//!
//! Authentication establishes [`RequestIdentity`]; this crate evaluates that identity against
//! declarative RBAC roles and bindings. It does not own HTTP parsing, policy persistence, or
//! admission policy.

use rusternetes_authn::RequestIdentity;

/// An authenticated actor that can appear in a Kubernetes role binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Subject {
    User(String),
    Group(String),
    ServiceAccount { namespace: String, name: String },
}

/// A reusable RBAC permission rule.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyRule {
    pub api_groups: Vec<String>,
    pub resources: Vec<String>,
    pub verbs: Vec<String>,
    pub resource_names: Vec<String>,
    pub non_resource_urls: Vec<String>,
}

/// A namespace-scoped RBAC role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Role {
    pub namespace: String,
    pub name: String,
    pub rules: Vec<PolicyRule>,
}

/// A cluster-scoped reusable RBAC role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRole {
    pub name: String,
    pub rules: Vec<PolicyRule>,
}

/// Role reference used by a namespace-scoped binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoleRef {
    Role(String),
    ClusterRole(String),
}

/// A binding whose grants are restricted to its namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleBinding {
    pub namespace: String,
    pub name: String,
    pub subjects: Vec<Subject>,
    pub role_ref: RoleRef,
}

/// A binding of a ClusterRole across all namespaces and cluster-scoped resources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRoleBinding {
    pub name: String,
    pub subjects: Vec<Subject>,
    pub role_ref: String,
}

/// The normalized target evaluated by RBAC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestTarget {
    Resource {
        api_group: String,
        resource: String,
        subresource: Option<String>,
        namespace: Option<String>,
        name: Option<String>,
    },
    NonResource {
        path: String,
    },
}

/// Request attributes shared by every authorizer implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationRequest {
    pub verb: String,
    pub target: RequestTarget,
}

impl AuthorizationRequest {
    pub fn resource(
        verb: impl Into<String>,
        api_group: impl Into<String>,
        resource: impl Into<String>,
        subresource: Option<String>,
        namespace: Option<String>,
        name: Option<String>,
    ) -> Self {
        Self {
            verb: verb.into(),
            target: RequestTarget::Resource {
                api_group: api_group.into(),
                resource: resource.into(),
                subresource,
                namespace,
                name,
            },
        }
    }

    pub fn non_resource(verb: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            verb: verb.into(),
            target: RequestTarget::NonResource { path: path.into() },
        }
    }
}

/// Additive Kubernetes RBAC policy evaluated with default deny.
#[derive(Clone, Debug, Default)]
pub struct RbacAuthorizer {
    roles: Vec<Role>,
    cluster_roles: Vec<ClusterRole>,
    role_bindings: Vec<RoleBinding>,
    cluster_role_bindings: Vec<ClusterRoleBinding>,
}

impl RbacAuthorizer {
    pub fn new(
        roles: Vec<Role>,
        cluster_roles: Vec<ClusterRole>,
        role_bindings: Vec<RoleBinding>,
        cluster_role_bindings: Vec<ClusterRoleBinding>,
    ) -> Self {
        Self {
            roles,
            cluster_roles,
            role_bindings,
            cluster_role_bindings,
        }
    }

    /// Returns true when at least one matching role rule grants the requested action.
    ///
    /// RBAC has no deny rules: missing or non-matching policy is intentionally false.
    pub fn authorize(&self, identity: &RequestIdentity, request: &AuthorizationRequest) -> bool {
        self.role_bindings.iter().any(|binding| {
            binding_matches(identity, &binding.subjects)
                && role_binding_applies_to_request(binding, request)
                && self
                    .rules_for_role_binding(binding)
                    .is_some_and(|rules| rules.iter().any(|rule| rule_matches(rule, request)))
        }) || self.cluster_role_bindings.iter().any(|binding| {
            binding_matches(identity, &binding.subjects)
                && self
                    .cluster_role(&binding.role_ref)
                    .is_some_and(|role| role.rules.iter().any(|rule| rule_matches(rule, request)))
        })
    }

    fn rules_for_role_binding(&self, binding: &RoleBinding) -> Option<&[PolicyRule]> {
        match &binding.role_ref {
            RoleRef::Role(name) => self
                .roles
                .iter()
                .find(|role| role.namespace == binding.namespace && role.name == *name)
                .map(|role| role.rules.as_slice()),
            RoleRef::ClusterRole(name) => self.cluster_role(name).map(|role| role.rules.as_slice()),
        }
    }

    fn cluster_role(&self, name: &str) -> Option<&ClusterRole> {
        self.cluster_roles.iter().find(|role| role.name == name)
    }
}

fn binding_matches(identity: &RequestIdentity, subjects: &[Subject]) -> bool {
    subjects.iter().any(|subject| match subject {
        Subject::User(name) => identity.username == *name,
        Subject::Group(name) => identity.groups.contains(name),
        Subject::ServiceAccount { namespace, name } => {
            identity.username == format!("system:serviceaccount:{namespace}:{name}")
        }
    })
}

fn role_binding_applies_to_request(binding: &RoleBinding, request: &AuthorizationRequest) -> bool {
    match &request.target {
        RequestTarget::Resource { namespace, .. } => {
            namespace.as_deref() == Some(&binding.namespace)
        }
        // Roles and RoleBindings cannot grant non-resource URL access.
        RequestTarget::NonResource { .. } => false,
    }
}

fn rule_matches(rule: &PolicyRule, request: &AuthorizationRequest) -> bool {
    if !matches_token(&rule.verbs, &request.verb) {
        return false;
    }
    match &request.target {
        RequestTarget::Resource {
            api_group,
            resource,
            subresource,
            name,
            ..
        } => {
            let resource = match subresource {
                Some(subresource) => format!("{resource}/{subresource}"),
                None => resource.clone(),
            };
            matches_token(&rule.api_groups, api_group)
                && matches_token(&rule.resources, &resource)
                && (rule.resource_names.is_empty()
                    || name
                        .as_deref()
                        .is_some_and(|name| matches_token(&rule.resource_names, name)))
        }
        RequestTarget::NonResource { path } => rule
            .non_resource_urls
            .iter()
            .any(|pattern| non_resource_url_matches(pattern, path)),
    }
}

fn matches_token(values: &[String], actual: &str) -> bool {
    values.iter().any(|value| value == "*" || value == actual)
}

fn non_resource_url_matches(pattern: &str, path: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => path.starts_with(prefix),
        None => pattern == path,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    fn identity(username: &str, groups: &[&str]) -> RequestIdentity {
        RequestIdentity {
            username: username.to_owned(),
            uid: None,
            groups: groups
                .iter()
                .map(|group| (*group).to_owned())
                .collect::<BTreeSet<_>>(),
            extra: BTreeMap::new(),
        }
    }

    fn config_map_rule(verbs: &[&str]) -> PolicyRule {
        PolicyRule {
            api_groups: vec![String::new()],
            resources: vec!["configmaps".to_owned()],
            verbs: verbs.iter().map(|verb| (*verb).to_owned()).collect(),
            ..PolicyRule::default()
        }
    }

    #[test]
    fn role_binding_is_limited_to_its_namespace_and_resource_names() {
        let mut rule = config_map_rule(&["get", "update"]);
        rule.resource_names = vec!["settings".to_owned()];
        let authorizer = RbacAuthorizer::new(
            vec![Role {
                namespace: "development".to_owned(),
                name: "settings-editor".to_owned(),
                rules: vec![rule],
            }],
            Vec::new(),
            vec![RoleBinding {
                namespace: "development".to_owned(),
                name: "bind-editor".to_owned(),
                subjects: vec![Subject::User("alice".to_owned())],
                role_ref: RoleRef::Role("settings-editor".to_owned()),
            }],
            Vec::new(),
        );
        let allowed = AuthorizationRequest::resource(
            "get",
            "",
            "configmaps",
            None,
            Some("development".to_owned()),
            Some("settings".to_owned()),
        );
        assert!(authorizer.authorize(&identity("alice", &[]), &allowed));

        let wrong_name = AuthorizationRequest::resource(
            "get",
            "",
            "configmaps",
            None,
            Some("development".to_owned()),
            Some("other".to_owned()),
        );
        assert!(!authorizer.authorize(&identity("alice", &[]), &wrong_name));
        let other_namespace = AuthorizationRequest::resource(
            "get",
            "",
            "configmaps",
            None,
            Some("production".to_owned()),
            Some("settings".to_owned()),
        );
        assert!(!authorizer.authorize(&identity("alice", &[]), &other_namespace));
    }

    #[test]
    fn cluster_role_binding_grants_group_across_namespaces_and_non_resource_paths() {
        let authorizer = RbacAuthorizer::new(
            Vec::new(),
            vec![ClusterRole {
                name: "viewer".to_owned(),
                rules: vec![
                    PolicyRule {
                        api_groups: vec![String::new()],
                        resources: vec!["configmaps".to_owned()],
                        verbs: vec!["get".to_owned(), "list".to_owned(), "watch".to_owned()],
                        ..PolicyRule::default()
                    },
                    PolicyRule {
                        non_resource_urls: vec!["/api".to_owned(), "/healthz/*".to_owned()],
                        verbs: vec!["get".to_owned()],
                        ..PolicyRule::default()
                    },
                ],
            }],
            Vec::new(),
            vec![ClusterRoleBinding {
                name: "team-viewers".to_owned(),
                subjects: vec![Subject::Group("team-a".to_owned())],
                role_ref: "viewer".to_owned(),
            }],
        );
        let list = AuthorizationRequest::resource(
            "list",
            "",
            "configmaps",
            None,
            Some("any".to_owned()),
            None,
        );
        assert!(authorizer.authorize(&identity("dana", &["team-a"]), &list));
        assert!(authorizer.authorize(
            &identity("dana", &["team-a"]),
            &AuthorizationRequest::non_resource("get", "/healthz/ping")
        ));
        assert!(!authorizer.authorize(
            &identity("dana", &["team-a"]),
            &AuthorizationRequest::non_resource("post", "/healthz/ping")
        ));
    }

    #[test]
    fn service_account_subject_and_unbound_identity_are_distinguished() {
        let authorizer = RbacAuthorizer::new(
            Vec::new(),
            vec![ClusterRole {
                name: "creator".to_owned(),
                rules: vec![config_map_rule(&["create"])],
            }],
            Vec::new(),
            vec![ClusterRoleBinding {
                name: "deploy-bot".to_owned(),
                subjects: vec![Subject::ServiceAccount {
                    namespace: "delivery".to_owned(),
                    name: "deploy-bot".to_owned(),
                }],
                role_ref: "creator".to_owned(),
            }],
        );
        let create = AuthorizationRequest::resource(
            "create",
            "",
            "configmaps",
            None,
            Some("production".to_owned()),
            None,
        );
        assert!(authorizer.authorize(
            &identity("system:serviceaccount:delivery:deploy-bot", &[]),
            &create
        ));
        assert!(!authorizer.authorize(&identity("system:anonymous", &[]), &create));
    }
}
