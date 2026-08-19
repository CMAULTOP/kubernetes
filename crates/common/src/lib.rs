//! Shared, typed error primitives for the Rusternetes control plane.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Kubernetes API status reasons supported by the first vertical slice.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StatusReason {
    #[serde(rename = "Success")]
    Success,
    #[serde(rename = "AlreadyExists")]
    AlreadyExists,
    #[serde(rename = "BadRequest")]
    BadRequest,
    #[serde(rename = "Conflict")]
    Conflict,
    #[serde(rename = "Expired")]
    Expired,
    #[serde(rename = "Forbidden")]
    Forbidden,
    #[serde(rename = "InternalError")]
    InternalError,
    #[serde(rename = "Invalid")]
    Invalid,
    #[serde(rename = "MethodNotAllowed")]
    MethodNotAllowed,
    #[serde(rename = "NotFound")]
    NotFound,
    #[serde(rename = "Unauthorized")]
    Unauthorized,
    #[serde(rename = "UnsupportedMediaType")]
    UnsupportedMediaType,
}

/// A typed reference to the resource named by an API operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceReference {
    pub group: String,
    pub resource: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
}

impl ResourceReference {
    pub fn config_map(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            group: String::new(),
            resource: "configmaps".to_owned(),
            namespace: Some(namespace.into()),
            name: Some(name.into()),
        }
    }

    pub fn pod(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            group: String::new(),
            resource: "pods".to_owned(),
            namespace: Some(namespace.into()),
            name: Some(name.into()),
        }
    }

    pub fn node(name: impl Into<String>) -> Self {
        Self {
            group: String::new(),
            resource: "nodes".to_owned(),
            namespace: None,
            name: Some(name.into()),
        }
    }

    pub fn namespace(name: impl Into<String>) -> Self {
        Self {
            group: String::new(),
            resource: "namespaces".to_owned(),
            namespace: None,
            name: Some(name.into()),
        }
    }
}

impl fmt::Display for ResourceReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            Some(name) => write!(formatter, "{} \"{}\"", self.resource, name),
            None => write!(formatter, "{}", self.resource),
        }
    }
}

/// An error that has an intentional Kubernetes API representation.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{resource} already exists")]
    AlreadyExists { resource: ResourceReference },
    #[error("invalid request: {message}")]
    BadRequest { message: String },
    #[error("the object has been modified; please apply your changes to the latest version and try again")]
    Conflict { resource: ResourceReference },
    #[error("forbidden: {message}")]
    Forbidden { message: String },
    #[error("invalid ConfigMap: {message}")]
    Invalid { message: String },
    #[error("resource version has expired: {message}")]
    ResourceExpired { message: String },
    #[error("{resource} not found")]
    NotFound { resource: ResourceReference },
    #[error("unauthorized: {message}")]
    Unauthorized { message: String },
    #[error("unsupported HTTP method: {method}")]
    MethodNotAllowed { method: String },
    #[error("unsupported media type: {media_type}")]
    UnsupportedMediaType { media_type: String },
    #[error("internal server error")]
    Internal,
}

impl ApiError {
    pub fn reason(&self) -> StatusReason {
        match self {
            Self::AlreadyExists { .. } => StatusReason::AlreadyExists,
            Self::BadRequest { .. } => StatusReason::BadRequest,
            Self::Conflict { .. } => StatusReason::Conflict,
            Self::Forbidden { .. } => StatusReason::Forbidden,
            Self::Invalid { .. } => StatusReason::Invalid,
            Self::ResourceExpired { .. } => StatusReason::Expired,
            Self::NotFound { .. } => StatusReason::NotFound,
            Self::Unauthorized { .. } => StatusReason::Unauthorized,
            Self::MethodNotAllowed { .. } => StatusReason::MethodNotAllowed,
            Self::UnsupportedMediaType { .. } => StatusReason::UnsupportedMediaType,
            Self::Internal => StatusReason::InternalError,
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::AlreadyExists { .. } | Self::Conflict { .. } => 409,
            Self::BadRequest { .. } => 400,
            Self::Invalid { .. } => 422,
            Self::Forbidden { .. } => 403,
            Self::ResourceExpired { .. } => 410,
            Self::NotFound { .. } => 404,
            Self::Unauthorized { .. } => 401,
            Self::MethodNotAllowed { .. } => 405,
            Self::UnsupportedMediaType { .. } => 415,
            Self::Internal => 500,
        }
    }

    pub fn details(&self) -> Option<&ResourceReference> {
        match self {
            Self::AlreadyExists { resource }
            | Self::Conflict { resource }
            | Self::NotFound { resource } => Some(resource),
            Self::BadRequest { .. }
            | Self::Forbidden { .. }
            | Self::Invalid { .. }
            | Self::ResourceExpired { .. }
            | Self::Unauthorized { .. }
            | Self::MethodNotAllowed { .. }
            | Self::UnsupportedMediaType { .. }
            | Self::Internal => None,
        }
    }
}
