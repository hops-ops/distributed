use super::*;

pub(super) fn validate_scope(
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
    scope: &ProjectionRecordScope,
) -> Result<(), ProjectionProtocolError> {
    if scope.topology() != topology {
        return Err(ProjectionProtocolError::ScopeMismatch {
            field: "projection record topology",
        });
    }
    if scope.projection_partition() != partition {
        return Err(ProjectionProtocolError::ScopeMismatch {
            field: "projection record partition",
        });
    }
    Ok(())
}

pub(super) fn bounded_name(
    field: &'static str,
    value: impl Into<String>,
    max: usize,
) -> Result<String, ProjectionProtocolError> {
    let value = value.into();
    if value.is_empty()
        || value.len() > max
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "{field} must contain 1..={max} non-whitespace, non-control UTF-8 bytes"
        )));
    }
    Ok(value)
}

pub(super) fn bounded_opaque(
    field: &'static str,
    value: impl Into<String>,
    max: usize,
) -> Result<String, ProjectionProtocolError> {
    let value = value.into();
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "{field} must contain 1..={max} non-control UTF-8 bytes"
        )));
    }
    Ok(value)
}

pub(in crate::projection_protocol) fn domain_separated_digest(
    domain: &[u8],
    bytes: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    digest.finalize().into()
}

pub(super) fn digest_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
