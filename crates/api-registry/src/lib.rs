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
            resources: vec![ResourceStrategy {
                plural: "configmaps",
                singular: "configmap",
                kind: "ConfigMap",
                scope: ResourceScope::Namespaced,
                verbs: &["create", "delete", "get", "list", "update", "watch"],
            }],
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

    /// Resolves the subset of Kubernetes core/v1 paths currently backed by a Rust strategy.
    pub fn resolve_core_v1_path(&self, path: &str) -> Option<ResolvedResourcePath> {
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let (plural, namespace, name) = match segments.as_slice() {
            ["api", "v1", plural] => (*plural, None, None),
            ["api", "v1", "namespaces", namespace, plural] => {
                (*plural, Some((*namespace).to_owned()), None)
            }
            ["api", "v1", "namespaces", namespace, plural, name] => (
                *plural,
                Some((*namespace).to_owned()),
                Some((*name).to_owned()),
            ),
            _ => return None,
        };
        let group_version = self.group_version("", "v1")?;
        let resource = group_version
            .resources
            .iter()
            .find(|resource| resource.plural == plural)?
            .clone();
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
        assert_eq!(discovery.resources.len(), 1);
        assert_eq!(discovery.resources[0].name, "configmaps");
        assert!(discovery.resources[0].verbs.contains(&"watch"));

        let route = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/configmaps/settings")
            .expect("ConfigMap path resolves");
        assert_eq!(route.resource.kind, "ConfigMap");
        assert_eq!(route.namespace.as_deref(), Some("development"));
        assert_eq!(route.name.as_deref(), Some("settings"));
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
