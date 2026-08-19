//! Typed API group/version registry for Rusternetes resource strategies.
//!
//! The registry is metadata and routing policy, not an unstructured object framework. Every entry
//! represents a resource with a concrete Rust implementation elsewhere in the workspace.

use std::{collections::BTreeSet, error::Error, fmt};

use rusternetes_api_types::{ApiResource, ApiResourceList, ApiVersions, ServerAddressByClientCidr};

/// Whether a Kubernetes resource lives in a namespace or at cluster scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceScope {
    Namespaced,
    Cluster,
}

impl ResourceScope {
    pub const fn is_namespaced(self) -> bool {
        matches!(self, Self::Namespaced)
    }
}

/// Fixed strategy metadata for one executable resource route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceStrategy {
    pub plural: &'static str,
    pub singular: &'static str,
    pub kind: &'static str,
    pub scope: ResourceScope,
    pub verbs: &'static [&'static str],
}

/// One served API group/version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupVersionStrategy {
    /// Empty only for Kubernetes's core API group.
    pub group: &'static str,
    pub version: &'static str,
    pub resources: Vec<ResourceStrategy>,
}

/// A route that has been resolved against the registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedResourcePath {
    pub group: &'static str,
    pub version: &'static str,
    pub resource: ResourceStrategy,
    pub namespace: Option<String>,
    pub name: Option<String>,
}

/// Configuration failure prevents registry construction before an API Server starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    DuplicateGroupVersion {
        group: String,
        version: String,
    },
    DuplicateResource {
        group: String,
        version: String,
        resource: String,
    },
    EmptyGroupVersion,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateGroupVersion { group, version } => {
                write!(
                    formatter,
                    "duplicate API group/version {group:?}/{version:?}"
                )
            }
            Self::DuplicateResource {
                group,
                version,
                resource,
            } => write!(
                formatter,
                "duplicate resource {resource:?} in API group/version {group:?}/{version:?}"
            ),
            Self::EmptyGroupVersion => write!(formatter, "API group version cannot be empty"),
        }
    }
}

impl Error for RegistryError {}

/// Immutable collection of executable API strategies.
#[derive(Clone, Debug)]
pub struct ApiRegistry {
    group_versions: Vec<GroupVersionStrategy>,
}

impl ApiRegistry {
    /// Builds a registry after rejecting ambiguous group/version or resource registrations.
    pub fn try_new(group_versions: Vec<GroupVersionStrategy>) -> Result<Self, RegistryError> {
        let mut versions = BTreeSet::new();
        for group_version in &group_versions {
            if group_version.version.is_empty() {
                return Err(RegistryError::EmptyGroupVersion);
            }
            let key = (group_version.group, group_version.version);
            if !versions.insert(key) {
                return Err(RegistryError::DuplicateGroupVersion {
                    group: group_version.group.to_owned(),
                    version: group_version.version.to_owned(),
                });
            }
            let mut resources = BTreeSet::new();
            for resource in &group_version.resources {
                if !resources.insert(resource.plural) {
                    return Err(RegistryError::DuplicateResource {
                        group: group_version.group.to_owned(),
                        version: group_version.version.to_owned(),
                        resource: resource.plural.to_owned(),
                    });
                }
            }
        }
        Ok(Self { group_versions })
    }

    /// The current executable Rusternetes API surface.
    pub fn core_v1() -> Self {
        Self::try_new(vec![GroupVersionStrategy {
            group: "",
            version: "v1",
            resources: vec![
                ResourceStrategy {
                    plural: "configmaps",
                    singular: "configmap",
                    kind: "ConfigMap",
                    scope: ResourceScope::Namespaced,
                    verbs: &["create", "delete", "get", "list", "update", "watch"],
                },
                ResourceStrategy {
                    plural: "pods",
                    singular: "pod",
                    kind: "Pod",
                    scope: ResourceScope::Namespaced,
                    verbs: &["create", "delete", "get", "list", "update", "watch"],
                },
                ResourceStrategy {
                    plural: "namespaces",
                    singular: "namespace",
                    kind: "Namespace",
                    scope: ResourceScope::Cluster,
                    verbs: &["create", "delete", "get", "list", "update", "watch"],
                },
            ],
        }])
        .expect("the built-in Rusternetes core/v1 registry is valid")
    }

    /// Produces Kubernetes core discovery strictly from registered core versions.
    pub fn core_api_versions(&self) -> ApiVersions {
        ApiVersions {
            kind: "APIVersions",
            api_version: "v1",
            versions: self
                .group_versions
                .iter()
                .filter(|group_version| group_version.group.is_empty())
                .map(|group_version| group_version.version)
                .collect(),
            server_address_by_client_cidrs: Vec::<ServerAddressByClientCidr>::new(),
        }
    }

    /// Produces Kubernetes resource discovery for one registered group/version.
    pub fn discovery(&self, group: &str, version: &str) -> Option<ApiResourceList> {
        let group_version = self.group_version(group, version)?;
        Some(ApiResourceList {
            kind: "APIResourceList",
            api_version: "v1",
            group_version: group_version.version,
            resources: group_version
                .resources
                .iter()
                .map(|resource| ApiResource {
                    name: resource.plural,
                    singular_name: resource.singular,
                    namespaced: resource.scope.is_namespaced(),
                    kind: resource.kind,
                    verbs: resource.verbs.to_vec(),
                })
                .collect(),
        })
    }

    /// Resolves executable core/v1 cluster- and namespace-scoped resource paths.
    pub fn resolve_core_v1_path(&self, path: &str) -> Option<ResolvedResourcePath> {
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let group_version = self.group_version("", "v1")?;
        let resolve = |plural: &str| {
            group_version
                .resources
                .iter()
                .find(|resource| resource.plural == plural)
                .cloned()
        };
        let (resource, namespace, name) = match segments.as_slice() {
            ["api", "v1", plural] => (resolve(plural)?, None, None),
            ["api", "v1", plural, name] => {
                let resource = resolve(plural)?;
                (
                    resource.clone(),
                    None,
                    matches!(resource.scope, ResourceScope::Cluster).then(|| (*name).to_owned()),
                )
            }
            ["api", "v1", "namespaces", namespace, plural] => {
                let resource = resolve(plural)?;
                (
                    resource.clone(),
                    matches!(resource.scope, ResourceScope::Namespaced)
                        .then(|| (*namespace).to_owned()),
                    None,
                )
            }
            ["api", "v1", "namespaces", namespace, plural, name] => {
                let resource = resolve(plural)?;
                (
                    resource.clone(),
                    matches!(resource.scope, ResourceScope::Namespaced)
                        .then(|| (*namespace).to_owned()),
                    matches!(resource.scope, ResourceScope::Namespaced).then(|| (*name).to_owned()),
                )
            }
            _ => return None,
        };
        match resource.scope {
            ResourceScope::Cluster if namespace.is_some() => return None,
            ResourceScope::Namespaced if segments.len() == 4 => return None,
            _ => {}
        }
        Some(ResolvedResourcePath {
            group: group_version.group,
            version: group_version.version,
            resource,
            namespace,
            name,
        })
    }

    fn group_version(&self, group: &str, version: &str) -> Option<&GroupVersionStrategy> {
        self.group_versions
            .iter()
            .find(|group_version| group_version.group == group && group_version.version == version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_registry_drives_discovery_and_path_resolution() {
        let registry = ApiRegistry::core_v1();
        assert_eq!(registry.core_api_versions().versions, vec!["v1"]);
        let discovery = registry.discovery("", "v1").expect("core v1 is registered");
        assert_eq!(discovery.resources.len(), 3);
        assert!(discovery
            .resources
            .iter()
            .any(|resource| resource.name == "configmaps" && resource.namespaced));
        assert!(discovery
            .resources
            .iter()
            .any(|resource| resource.name == "pods" && resource.namespaced));
        assert!(discovery
            .resources
            .iter()
            .any(|resource| resource.name == "namespaces" && !resource.namespaced));

        let route = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/configmaps/settings")
            .expect("ConfigMap path resolves");
        assert_eq!(route.resource.kind, "ConfigMap");
        assert_eq!(route.namespace.as_deref(), Some("development"));
        assert_eq!(route.name.as_deref(), Some("settings"));

        let namespace = registry
            .resolve_core_v1_path("/api/v1/namespaces/development")
            .expect("cluster-scoped Namespace path resolves");
        assert_eq!(namespace.resource.kind, "Namespace");
        assert_eq!(namespace.namespace, None);
        assert_eq!(namespace.name.as_deref(), Some("development"));

        let pod_collection = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/pods")
            .expect("namespaced Pod collection resolves");
        assert_eq!(pod_collection.resource.kind, "Pod");
        assert_eq!(pod_collection.namespace.as_deref(), Some("development"));
    }

    #[test]
    fn duplicate_resource_or_group_version_fails_before_startup() {
        let resource = ResourceStrategy {
            plural: "widgets",
            singular: "widget",
            kind: "Widget",
            scope: ResourceScope::Cluster,
            verbs: &["get"],
        };
        let duplicate_resource = ApiRegistry::try_new(vec![GroupVersionStrategy {
            group: "example.io",
            version: "v1",
            resources: vec![resource.clone(), resource],
        }]);
        assert!(matches!(
            duplicate_resource,
            Err(RegistryError::DuplicateResource { .. })
        ));

        let duplicate_version = ApiRegistry::try_new(vec![
            GroupVersionStrategy {
                group: "example.io",
                version: "v1",
                resources: Vec::new(),
            },
            GroupVersionStrategy {
                group: "example.io",
                version: "v1",
                resources: Vec::new(),
            },
        ]);
        assert!(matches!(
            duplicate_version,
            Err(RegistryError::DuplicateGroupVersion { .. })
        ));
    }
}
