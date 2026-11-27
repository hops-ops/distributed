//! Canonical bytes, encoding and fingerprints.

use super::*;

impl ApplicationManifest {
    /// Return exact deterministic manifest bytes, including the explicit
    /// schema version and complete fingerprint material.
    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        // Encoding is also an acceptance boundary: an in-memory attacker may
        // not manufacture a byte artifact with empty nested fingerprints and
        // rely on the outer manifest fingerprint to make it look complete.
        self.validate_inner(true, false)?;
        let mut canonical = self.clone();
        canonical.canonicalize_collections();
        canonical.fingerprints = ManifestFingerprint::default();
        let mut logical = canonical.clone();
        logical.provenance = ManifestProvenance::default();
        logical
            .extensions
            .retain(|extension| extension.id != FRAMEWORK_COMPATIBILITY_EXTENSION_ID);
        let logical_bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&logical)?))?;
        canonical.fingerprints.logical = sha256_fingerprint(&logical_bytes);
        let canonical_without_canonical =
            serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
        canonical.fingerprints.canonical = sha256_fingerprint(&canonical_without_canonical);
        let bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
        if bytes.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest exceeds {MAX_APPLICATION_MANIFEST_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    pub fn refresh_fingerprints(&mut self) -> ApplicationResult<()> {
        self.fingerprints = ManifestFingerprint::default();
        let bytes = self.canonical_bytes()?;
        let canonical: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ApplicationError::Canonical(error.to_string()))?;
        self.fingerprints = canonical.fingerprints;
        Ok(())
    }

    pub fn encode(&self) -> ApplicationResult<Vec<u8>> {
        self.canonical_bytes()
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> ApplicationResult<Self> {
        if bytes.is_empty() || bytes.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest bytes must be between 1 and {MAX_APPLICATION_MANIFEST_BYTES}"
            )));
        }
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|error| ApplicationError::Canonical(error.to_string()))?;
        if manifest.schema_version != APPLICATION_MANIFEST_SCHEMA_VERSION {
            return Err(ApplicationError::UnsupportedVersion {
                expected: APPLICATION_MANIFEST_SCHEMA_VERSION,
                actual: manifest.schema_version,
            });
        }
        manifest.validate_inner(true, true)?;
        if manifest.canonical_bytes()? != bytes {
            return Err(ApplicationError::NonCanonical("application manifest"));
        }
        Ok(manifest)
    }

    pub fn decode(bytes: &[u8]) -> ApplicationResult<Self> {
        Self::from_canonical_bytes(bytes)
    }

    pub fn fingerprint(&self) -> ApplicationResult<String> {
        let bytes = self.canonical_bytes()?;
        let manifest = Self::from_canonical_bytes(&bytes)?;
        Ok(manifest.fingerprints.canonical)
    }

    /// Return the logical identity, excluding generator/source provenance.
    /// The canonical artifact fingerprint still changes when those fields do.
    pub fn logical_fingerprint(&self) -> ApplicationResult<String> {
        let bytes = self.canonical_bytes()?;
        let manifest = Self::from_canonical_bytes(&bytes)?;
        Ok(manifest.fingerprints.logical)
    }
}

pub(super) fn expected_fingerprints(
    manifest: &ApplicationManifest,
) -> ApplicationResult<ManifestFingerprint> {
    let mut canonical = manifest.clone();
    canonical.canonicalize_collections();
    canonical.fingerprints = ManifestFingerprint::default();
    let mut logical = canonical.clone();
    logical.provenance = ManifestProvenance::default();
    logical
        .extensions
        .retain(|extension| extension.id != FRAMEWORK_COMPATIBILITY_EXTENSION_ID);
    let logical_bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&logical)?))?;
    let logical = sha256_fingerprint(&logical_bytes);
    canonical.fingerprints.logical = logical.clone();
    let with_logical = serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
    let canonical_fingerprint = sha256_fingerprint(&with_logical);
    Ok(ManifestFingerprint {
        logical,
        canonical: canonical_fingerprint,
    })
}

pub(super) fn validate_fingerprint<T: Serialize>(
    kind: &'static str,
    identity: &str,
    fingerprint: &str,
    value: &T,
    required: bool,
) -> ApplicationResult<()> {
    if fingerprint.is_empty() {
        if required {
            return Err(ApplicationError::NonCanonical(match kind {
                "module" => "module fingerprint",
                "surface" => "surface fingerprint",
                "projection" => "projection fingerprint",
                "model" => "model fingerprint",
                "command" => "command fingerprint",
                _ => "nested fingerprint",
            }));
        }
        return Ok(());
    }
    let mut value = serde_json::to_value(value)?;
    if let serde_json::Value::Object(fields) = &mut value {
        fields.insert(
            "fingerprint".into(),
            serde_json::Value::String(String::new()),
        );
    }
    let expected = sha256_fingerprint(&serde_json::to_vec(&canonical_json(&value))?);
    if expected != fingerprint {
        return Err(ApplicationError::NonCanonical(match kind {
            "module" => "module fingerprint",
            "surface" => "surface fingerprint",
            "projection" => "projection fingerprint",
            "model" => "model fingerprint",
            "command" => "command fingerprint",
            _ => "nested fingerprint",
        }));
    }
    let _ = identity;
    Ok(())
}
