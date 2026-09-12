use super::{response, Body, GatewayError, HeaderValue, Request, Response, StatusCode};
use axum::http::header;
use std::{collections::BTreeMap, sync::Arc};

/// Preloaded asset with explicit response content type.
#[derive(Clone)]
pub struct Asset {
    /// Bytes stored once and shared between responses.
    pub bytes: axum::body::Bytes,
    /// Trusted application-selected content type.
    pub content_type: HeaderValue,
}
/// Immutable bounded static inventory. There is no fallback filesystem path,
/// symlink traversal or directory listing. Route policy runs before lookup.
#[derive(Clone)]
pub struct StaticAssets(Arc<BTreeMap<String, Asset>>);
impl StaticAssets {
    /// Validate canonical paths, duplicate entries and a total memory budget.
    pub fn new(
        assets: impl IntoIterator<Item = (String, Asset)>,
        max_bytes: usize,
    ) -> Result<Self, GatewayError> {
        let mut inventory = BTreeMap::new();
        let mut bytes = 0usize;
        for (path, asset) in assets {
            let normalized = super::super::route::normalize_path(&path)?;
            if path != normalized || inventory.len() >= 16384 {
                return Err(GatewayError("invalid static asset inventory"));
            }
            bytes = bytes
                .checked_add(asset.bytes.len())
                .ok_or(GatewayError("asset inventory too large"))?;
            if bytes > max_bytes || inventory.insert(path, asset).is_some() {
                return Err(GatewayError("asset inventory budget or duplicate"));
            }
        }
        Ok(Self(Arc::new(inventory)))
    }
    pub(super) fn serve(&self, request: Request<Body>) -> Response {
        if request.method() != "GET" && request.method() != "HEAD" {
            let mut result = response(StatusCode::METHOD_NOT_ALLOWED);
            result
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return result;
        }
        let Ok(path) = super::super::route::normalize_path(request.uri().path()) else {
            return response(StatusCode::BAD_REQUEST);
        };
        let Some(asset) = self.0.get(&path) else {
            return response(StatusCode::NOT_FOUND);
        };
        let mut result = response(StatusCode::OK);
        result
            .headers_mut()
            .insert(header::CONTENT_TYPE, asset.content_type.clone());
        result.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&asset.bytes.len().to_string()).expect("usize header"),
        );
        result.headers_mut().insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        if request.method() != "HEAD" {
            *result.body_mut() = Body::from(asset.bytes.clone());
        }
        result
    }
}
