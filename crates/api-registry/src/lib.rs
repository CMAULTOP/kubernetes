//! Typed API group/version registry for Rusternetes resource strategies.
//!
//! The registry is metadata and routing policy, not an unstructured object framework. Every entry
//! represents a resource with a concrete Rust implementation elsewhere in the workspace.

use std::{collections::BTreeSet, error::Error, fmt};

use rusternetes_api_types::{
    ApiGroup, ApiGroupList, ApiResource, ApiResourceList, ApiVersions, GroupVersionForDiscovery,
    ServerAddressByClientCidr,
};

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

/// Fixed discovery and authorization metadata for one executable resource subresource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubresourceStrategy {
    /// URI path component below a named parent resource.
    pub name: &'static str,
    /// Kubernetes discovery name, including the parent resource segment.
    pub discovery_name: &'static str,
    pub kind: &'static str,
    pub verbs: &'static [&'static str],
}

/// Fixed strategy metadata for one executable resource route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceStrategy {
    pub plural: &'static str,
    pub singular: &'static str,
    pub kind: &'static str,
    pub scope: ResourceScope,
    pub verbs: &'static [&'static str],
    pub subresources: &'static [SubresourceStrategy],
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
    pub subresource: Option<&'static str>,
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
        Self::try_new(vec![
            GroupVersionStrategy {
                group: "",
                version: "v1",
                resources: vec![
                    ResourceStrategy {
                        plural: "configmaps",
                        singular: "configmap",
                        kind: "ConfigMap",
                        scope: ResourceScope::Namespaced,
                        verbs: &["create", "delete", "get", "list", "update", "watch"],
                        subresources: &[],
                    },
                    ResourceStrategy {
                        plural: "pods",
                        singular: "pod",
                        kind: "Pod",
                        scope: ResourceScope::Namespaced,
                        verbs: &["create", "delete", "get", "list", "update", "watch"],
                        subresources: &[SubresourceStrategy {
                            name: "status",
                            discovery_name: "pods/status",
                            kind: "Pod",
                            verbs: &["get", "update"],
                        }],
                    },
                    ResourceStrategy {
                        plural: "serviceaccounts",
                        singular: "serviceaccount",
                        kind: "ServiceAccount",
                        scope: ResourceScope::Namespaced,
                        verbs: &["create", "delete", "get", "list", "update", "watch"],
                        subresources: &[SubresourceStrategy {
                            name: "token",
                            discovery_name: "serviceaccounts/token",
                            kind: "TokenRequest",
                            verbs: &["create"],
                        }],
                    },
                    ResourceStrategy {
                        plural: "nodes",
                        singular: "node",
                        kind: "Node",
                        scope: ResourceScope::Cluster,
                        verbs: &[
                            "create", "delete", "get", "list", "patch", "update", "watch",
                        ],
                        subresources: &[SubresourceStrategy {
                            name: "status",
                            discovery_name: "nodes/status",
                            kind: "Node",
                            verbs: &["get", "update"],
                        }],
                    },
                    ResourceStrategy {
                        plural: "namespaces",
                        singular: "namespace",
                        kind: "Namespace",
                        scope: ResourceScope::Cluster,
                        verbs: &["create", "delete", "get", "list", "update", "watch"],
                        subresources: &[
                            SubresourceStrategy {
                                name: "status",
                                discovery_name: "namespaces/status",
                                kind: "Namespace",
                                verbs: &["get", "update"],
                            },
                            SubresourceStrategy {
                                name: "finalize",
                                discovery_name: "namespaces/finalize",
                                kind: "Namespace",
                                verbs: &["update"],
                            },
                        ],
                    },
                ],
            },
            GroupVersionStrategy {
                group: "authentication.k8s.io",
                version: "v1",
                resources: vec![ResourceStrategy {
                    plural: "tokenreviews",
                    singular: "tokenreview",
                    kind: "TokenReview",
                    scope: ResourceScope::Cluster,
                    verbs: &["create"],
                    subresources: &[],
                }],
            },
            GroupVersionStrategy {
                group: "authorization.k8s.io",
                version: "v1",
                resources: vec![
                    ResourceStrategy {
                        plural: "selfsubjectaccessreviews",
                        singular: "selfsubjectaccessreview",
                        kind: "SelfSubjectAccessReview",
                        scope: ResourceScope::Cluster,
                        verbs: &["create"],
                        subresources: &[],
                    },
                    ResourceStrategy {
                        plural: "subjectaccessreviews",
                        singular: "subjectaccessreview",
                        kind: "SubjectAccessReview",
                        scope: ResourceScope::Cluster,
                        verbs: &["create"],
                        subresources: &[],
                    },
                ],
            },
        ])
        .expect("the built-in Rusternetes API registry is valid")
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

    /// Produces discovery for all registered non-core API groups.
    pub fn named_api_groups(&self) -> ApiGroupList {
        let groups = self
            .group_versions
            .iter()
            .filter(|group_version| !group_version.group.is_empty())
            .fold(Vec::<ApiGroup>::new(), |mut groups, group_version| {
                if let Some(group) = groups
                    .iter_mut()
                    .find(|group| group.name == group_version.group)
                {
                    group.versions.push(GroupVersionForDiscovery {
                        group_version: format!("{}/{}", group_version.group, group_version.version),
                        version: group_version.version,
                    });
                } else {
                    let version = GroupVersionForDiscovery {
                        group_version: format!("{}/{}", group_version.group, group_version.version),
                        version: group_version.version,
                    };
                    groups.push(ApiGroup {
                        name: group_version.group,
                        versions: vec![version.clone()],
                        preferred_version: version,
                    });
                }
                groups
            });
        ApiGroupList {
            kind: "APIGroupList",
            api_version: "v1",
            groups,
        }
    }

    /// Produces discovery for one registered non-core API group.
    pub fn named_api_group(&self, name: &str) -> Option<ApiGroup> {
        self.named_api_groups()
            .groups
            .into_iter()
            .find(|group| group.name == name)
    }

    /// Produces Kubernetes resource discovery for one registered group/version.
    pub fn discovery(&self, group: &str, version: &str) -> Option<ApiResourceList> {
        let group_version = self.group_version(group, version)?;
        Some(ApiResourceList {
            kind: "APIResourceList",
            api_version: "v1",
            group_version: if group_version.group.is_empty() {
                group_version.version.to_owned()
            } else {
                format!("{}/{}", group_version.group, group_version.version)
            },
            resources: group_version
                .resources
                .iter()
                .flat_map(|resource| {
                    std::iter::once(ApiResource {
                        name: resource.plural,
                        singular_name: resource.singular,
                        namespaced: resource.scope.is_namespaced(),
                        kind: resource.kind,
                        verbs: resource.verbs.to_vec(),
                    })
                    .chain(resource.subresources.iter().map(
                        move |subresource| ApiResource {
                            name: subresource.discovery_name,
                            singular_name: "",
                            namespaced: resource.scope.is_namespaced(),
                            kind: subresource.kind,
                            verbs: subresource.verbs.to_vec(),
                        },
                    ))
                })
                .collect(),
        })
    }

    /// Resolves any currently executable API resource path for authorization metadata.
    pub fn resolve_path(&self, path: &str) -> Option<ResolvedResourcePath> {
        if path.starts_with("/api/v1") {
            return self.resolve_core_v1_path(path);
        }
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let (group, version, plural, name) = match segments.as_slice() {
            ["apis", group, version, plural] => (*group, *version, *plural, None),
            ["apis", group, version, plural, name] => (*group, *version, *plural, Some(*name)),
            _ => return None,
        };
        let group_version = self.group_version(group, version)?;
        let resource = group_version
            .resources
            .iter()
            .find(|resource| resource.plural == plural)?
            .clone();
        if matches!(resource.scope, ResourceScope::Namespaced) {
            return None;
        }
        Some(ResolvedResourcePath {
            group: group_version.group,
            version: group_version.version,
            resource,
            namespace: None,
            name: name.map(str::to_owned),
            subresource: None,
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
        let (resource, namespace, name, subresource) = match segments.as_slice() {
            ["api", "v1", plural] => (resolve(plural)?, None, None, None),
            ["api", "v1", plural, name] => {
                let resource = resolve(plural)?;
                (
                    resource.clone(),
                    None,
                    matches!(resource.scope, ResourceScope::Cluster).then(|| (*name).to_owned()),
                    None,
                )
            }
            ["api", "v1", "namespaces", namespace_or_name, final_segment] => {
                let namespace_resource = resolve("namespaces")?;
                if let Some(subresource) = namespace_resource
                    .subresources
                    .iter()
                    .find(|strategy| strategy.name == *final_segment)
                {
                    (
                        namespace_resource.clone(),
                        None,
                        matches!(namespace_resource.scope, ResourceScope::Cluster)
                            .then(|| (*namespace_or_name).to_owned()),
                        Some(subresource.name),
                    )
                } else {
                    let resource = resolve(final_segment)?;
                    (
                        resource.clone(),
                        matches!(resource.scope, ResourceScope::Namespaced)
                            .then(|| (*namespace_or_name).to_owned()),
                        None,
                        None,
                    )
                }
            }
            ["api", "v1", "namespaces", namespace, plural, name] => {
                let resource = resolve(plural)?;
                (
                    resource.clone(),
                    matches!(resource.scope, ResourceScope::Namespaced)
                        .then(|| (*namespace).to_owned()),
                    matches!(resource.scope, ResourceScope::Namespaced).then(|| (*name).to_owned()),
                    None,
                )
            }
            ["api", "v1", "namespaces", namespace, plural, name, subresource] => {
                let resource = resolve(plural)?;
                let subresource = resource
                    .subresources
                    .iter()
                    .find(|strategy| strategy.name == *subresource)?;
                (
                    resource.clone(),
                    matches!(resource.scope, ResourceScope::Namespaced)
                        .then(|| (*namespace).to_owned()),
                    matches!(resource.scope, ResourceScope::Namespaced).then(|| (*name).to_owned()),
                    Some(subresource.name),
                )
            }
            ["api", "v1", plural, name, subresource] => {
                let resource = resolve(plural)?;
                let subresource = resource
                    .subresources
                    .iter()
                    .find(|strategy| strategy.name == *subresource)?;
                (
                    resource.clone(),
                    None,
                    matches!(resource.scope, ResourceScope::Cluster).then(|| (*name).to_owned()),
                    Some(subresource.name),
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
            subresource,
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
        assert_eq!(discovery.resources.len(), 10);
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
            .any(|resource| resource.name == "serviceaccounts" && resource.namespaced));
        assert!(discovery
            .resources
            .iter()
            .any(|resource| resource.name == "nodes" && !resource.namespaced));
        assert!(discovery.resources.iter().any(|resource| {
            resource.name == "nodes/status"
                && !resource.namespaced
                && resource.verbs == ["get", "update"]
        }));
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

        let node = registry
            .resolve_core_v1_path("/api/v1/nodes/node-a")
            .expect("cluster-scoped Node path resolves");
        assert_eq!(node.resource.kind, "Node");
        assert_eq!(node.namespace, None);
        assert_eq!(node.name.as_deref(), Some("node-a"));

        let node_status = registry
            .resolve_core_v1_path("/api/v1/nodes/node-a/status")
            .expect("cluster-scoped Node status path resolves");
        assert_eq!(node_status.resource.kind, "Node");
        assert_eq!(node_status.name.as_deref(), Some("node-a"));
        assert_eq!(node_status.subresource, Some("status"));

        let namespace = registry
            .resolve_core_v1_path("/api/v1/namespaces/development")
            .expect("cluster-scoped Namespace path resolves");
        assert_eq!(namespace.resource.kind, "Namespace");
        assert_eq!(namespace.namespace, None);
        assert_eq!(namespace.name.as_deref(), Some("development"));
        let namespace_finalize = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/finalize")
            .expect("cluster-scoped Namespace finalize path resolves");
        assert_eq!(namespace_finalize.resource.kind, "Namespace");
        assert_eq!(namespace_finalize.name.as_deref(), Some("development"));
        assert_eq!(namespace_finalize.subresource, Some("finalize"));
        assert!(discovery.resources.iter().any(|resource| {
            resource.name == "namespaces/finalize"
                && !resource.namespaced
                && resource.kind == "Namespace"
                && resource.verbs == ["update"]
        }));

        let pod_collection = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/pods")
            .expect("namespaced Pod collection resolves");
        assert_eq!(pod_collection.resource.kind, "Pod");
        assert_eq!(pod_collection.namespace.as_deref(), Some("development"));

        let pod_status = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/pods/web/status")
            .expect("namespaced Pod status subresource resolves");
        assert_eq!(pod_status.resource.kind, "Pod");
        assert_eq!(pod_status.namespace.as_deref(), Some("development"));
        assert_eq!(pod_status.name.as_deref(), Some("web"));
        assert_eq!(pod_status.subresource, Some("status"));
        assert!(discovery.resources.iter().any(|resource| {
            resource.name == "pods/status"
                && resource.namespaced
                && resource.kind == "Pod"
                && resource.verbs == ["get", "update"]
        }));

        let service_account = registry
            .resolve_core_v1_path("/api/v1/namespaces/development/serviceaccounts/build-robot")
            .expect("namespaced ServiceAccount path resolves");
        assert_eq!(service_account.resource.kind, "ServiceAccount");
        assert_eq!(service_account.namespace.as_deref(), Some("development"));
        assert_eq!(service_account.name.as_deref(), Some("build-robot"));

        let token = registry
            .resolve_core_v1_path(
                "/api/v1/namespaces/development/serviceaccounts/build-robot/token",
            )
            .expect("namespaced ServiceAccount token subresource resolves");
        assert_eq!(token.resource.kind, "ServiceAccount");
        assert_eq!(token.namespace.as_deref(), Some("development"));
        assert_eq!(token.name.as_deref(), Some("build-robot"));
        assert_eq!(token.subresource, Some("token"));
        assert!(discovery.resources.iter().any(|resource| {
            resource.name == "serviceaccounts/token"
                && resource.namespaced
                && resource.kind == "TokenRequest"
                && resource.verbs == ["create"]
        }));
    }

    #[test]
    fn duplicate_resource_or_group_version_fails_before_startup() {
        let resource = ResourceStrategy {
            plural: "widgets",
            singular: "widget",
            kind: "Widget",
            scope: ResourceScope::Cluster,
            verbs: &["get"],
            subresources: &[],
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
