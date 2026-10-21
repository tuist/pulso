//! Validate the versioned typed-attribute envelope without BEAM terms.
//! Absent/null metadata is explicitly legacy, never reconstructed from strings.
use crate::json_read::{Fallback, Parser, RawFields, Res};
use std::collections::BTreeSet;

fn object(raw: &[u8]) -> Res<RawFields<'_>> {
    let mut parser = Parser::new(raw);
    let value = parser.raw_object()?;
    parser.ws();
    parser.end()?;
    Ok(value)
}
fn array(raw: &[u8]) -> Res<Vec<&[u8]>> {
    let mut parser = Parser::new(raw);
    let value = parser.raw_array()?;
    parser.ws();
    parser.end()?;
    Ok(value)
}
fn string(raw: &[u8]) -> Res<std::borrow::Cow<'_, [u8]>> {
    let mut parser = Parser::new(raw);
    let value = parser.string()?;
    parser.ws();
    parser.end()?;
    Ok(value)
}
fn field<'a>(fields: &RawFields<'a>, name: &[u8]) -> Res<&'a [u8]> {
    fields
        .iter()
        .find(|(key, _)| key.as_ref() == name)
        .map(|(_, value)| *value)
        .ok_or(Fallback)
}

pub fn validate(payload: &[u8]) -> Res<()> {
    validated_fields(payload).map(|_| ())
}
pub(crate) fn validated_fields(payload: &[u8]) -> Res<RawFields<'_>> {
    let fields = object(payload)?;
    if fields
        .iter()
        .filter(|(key, _)| key.as_ref() == b"otlp")
        .count()
        > 1
    {
        return Err(Fallback);
    }
    let Some((_, metadata)) = fields.iter().find(|(key, _)| key.as_ref() == b"otlp") else {
        return Ok(fields);
    };
    if *metadata == b"null" {
        return Ok(fields);
    }
    let metadata = object(metadata)?;
    if metadata.len() != 6 || field(&metadata, b"version")? != b"1" {
        return Err(Fallback);
    }
    for name in [b"resource".as_slice(), b"scope", b"attributes"] {
        attributes(field(&metadata, name)?, 0)?;
    }
    for name in [b"events".as_slice(), b"links"] {
        let owners = array(field(&metadata, name)?)?;
        if owners.len() != array(field(&fields, name)?)?.len() {
            return Err(Fallback);
        }
        for owner in owners {
            attributes(owner, 0)?;
        }
    }
    Ok(fields)
}
fn attributes(raw: &[u8], depth: usize) -> Res<()> {
    let mut keys = BTreeSet::new();
    for pair in array(raw)? {
        let pair = object(pair)?;
        if pair.len() != 2 {
            return Err(Fallback);
        }
        let key = string(field(&pair, b"key")?)?;
        if key.is_empty() || !keys.insert(key.into_owned()) {
            return Err(Fallback);
        }
        any_value(field(&pair, b"value")?, depth)?;
    }
    Ok(())
}
fn any_value(raw: &[u8], depth: usize) -> Res<()> {
    if depth > 128 {
        return Err(Fallback);
    }
    let fields = object(raw)?;
    if fields.is_empty() {
        return Ok(());
    }
    if fields.len() != 1 {
        return Err(Fallback);
    }
    let (key, value) = &fields[0];
    match key.as_ref() {
        b"stringValue" => {
            string(value)?;
        }
        b"boolValue" if *value == b"true" || *value == b"false" => {}
        b"intValue" => {
            let value = string(value)?;
            let value = std::str::from_utf8(&value).map_err(|_| Fallback)?;
            let number: i64 = value.parse().map_err(|_| Fallback)?;
            if number.to_string() != value {
                return Err(Fallback);
            }
        }
        b"doubleValue" => {
            double(value)?;
        }
        b"bytesValue" => {
            if !base64(&string(value)?) {
                return Err(Fallback);
            }
        }
        b"arrayValue" => {
            let owner = object(value)?;
            if owner.len() != 1 {
                return Err(Fallback);
            }
            for value in array(field(&owner, b"values")?)? {
                any_value(value, depth + 1)?;
            }
        }
        b"kvlistValue" => {
            let owner = object(value)?;
            if owner.len() != 1 {
                return Err(Fallback);
            }
            attributes(field(&owner, b"values")?, depth + 1)?;
        }
        _ => return Err(Fallback),
    }
    Ok(())
}
// OTLP's three quoted non-finite spellings retain type without BEAM floats.
// Finite stored doubles remain JSON numbers; do not accept arbitrary strings.
pub(crate) fn double(raw: &[u8]) -> Res<f64> {
    if raw.first() == Some(&b'"') {
        return match string(raw)?.as_ref() {
            b"NaN" => Ok(f64::NAN),
            b"Infinity" => Ok(f64::INFINITY),
            b"-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(Fallback),
        };
    }
    let key = crate::trace_sort_key::key(raw)?;
    if !matches!(key.first(), Some(3 | 4)) {
        return Err(Fallback);
    }
    let value: f64 = std::str::from_utf8(raw)
        .map_err(|_| Fallback)?
        .parse()
        .map_err(|_| Fallback)?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(Fallback)
    }
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}
fn base64(value: &[u8]) -> bool {
    if !value.len().is_multiple_of(4) {
        return false;
    }
    let padding = value.iter().rev().take_while(|&&byte| byte == b'=').count();
    if padding > 2 {
        return false;
    }
    let digits = &value[..value.len() - padding];
    if !digits.iter().all(|&byte| digit(byte).is_some()) {
        return false;
    }
    match padding {
        1 => digits
            .last()
            .and_then(|&byte| digit(byte))
            .is_some_and(|value| value & 3 == 0),
        2 => digits
            .last()
            .and_then(|&byte| digit(byte))
            .is_some_and(|value| value & 15 == 0),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_closed_typed_envelopes() {
        assert!(validate(br#"{"attributes":{"bytes":"AQ=="}}"#).is_ok());
        let valid = br#"{"events":[],"links":[],"otlp":{"version":1,"resource":[],"scope":[],"attributes":[{"key":"b","value":{"bytesValue":"AQ=="}},{"key":"s","value":{"stringValue":"AQ=="}}],"events":[],"links":[]}}"#;
        assert!(validate(valid).is_ok());
        for metadata in ["null", "[]", "1", r#"{"version":2}"#] {
            let value = format!(r#"{{"otlp":{metadata}}}"#);
            assert_eq!(validate(value.as_bytes()).is_ok(), metadata == "null");
        }
    }
    #[test]
    fn canonical_values_only() {
        for value in [
            r#"{"bytesValue":"AR=="}"#,
            r#"{"intValue":"9223372036854775808"}"#,
            r#"{"intValue":"01"}"#,
            r#"{"doubleValue":"nan"}"#,
            r#"{"doubleValue":"+Inf"}"#,
            r#"{"doubleValue":"1.0"}"#,
            r#"{"boolValue":0}"#,
            r#"{"stringValue":"s","bytesValue":"AQ=="}"#,
        ] {
            assert!(any_value(value.as_bytes(), 0).is_err());
        }
        assert!(any_value(br#"{"intValue":"-9223372036854775808"}"#, 0).is_ok());
        assert!(any_value(br#"{"doubleValue":-0.0}"#, 0).is_ok());
        for spelling in ["NaN", "Infinity", "-Infinity"] {
            assert!(any_value(format!(r#"{{"doubleValue":"{spelling}"}}"#).as_bytes(), 0).is_ok());
        }
        assert!(double(br#""NaN""#).unwrap().is_nan());
        assert_eq!(double(br#""Infinity""#).unwrap(), f64::INFINITY);
        assert_eq!(double(br#""-Infinity""#).unwrap(), f64::NEG_INFINITY);
        assert_eq!(double(b"-0.0").unwrap().to_bits(), (-0.0f64).to_bits());
        assert!(attributes(br#"[{"key":"a","value":{}},{"key":"a","value":{}}]"#, 0).is_err());
    }
}
