//! Allocation-light preflight of ingest protobufs. Count raw entries before
//! malformed records or duplicate keys can disappear in the real decoder.
use crate::{
    labels,
    wire::{Reader, Value},
};

pub type Options = (usize, usize, usize, usize, usize);

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidProtobuf,
    TooManyRecords,
    AttributesTooLarge,
}

pub struct Limits {
    records: usize,
    attributes: usize,
    key_bytes: usize,
    value_bytes: usize,
    attribute_bytes: usize,
}

impl From<Options> for Limits {
    fn from(v: Options) -> Self {
        Self {
            records: v.0,
            attributes: v.1,
            key_bytes: v.2,
            value_bytes: v.3,
            attribute_bytes: v.4,
        }
    }
}

fn fields<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(u64, Value<'a>) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut reader = Reader::new(input);
    while let Some((field, value)) = reader.next_field().ok_or(Error::InvalidProtobuf)? {
        visit(field, value)?;
    }
    Ok(())
}

fn increment(count: &mut usize, max: usize) -> Result<(), Error> {
    if *count >= max {
        return Err(Error::TooManyRecords);
    }
    *count += 1;
    Ok(())
}

impl Limits {
    fn pair(
        &self,
        key: &[u8],
        value: &[u8],
        count: &mut usize,
        bytes: &mut usize,
    ) -> Result<(), Error> {
        if *count >= self.attributes || key.len() > self.key_bytes || value.len() > self.value_bytes
        {
            return Err(Error::AttributesTooLarge);
        }
        *count += 1;
        *bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
        if *bytes > self.attribute_bytes {
            return Err(Error::AttributesTooLarge);
        }
        Ok(())
    }

    fn wire_pair(&self, input: &[u8], count: &mut usize, bytes: &mut usize) -> Result<(), Error> {
        let (mut key, mut value): (&[u8], &[u8]) = (b"", b"");
        fields(input, |field, v| {
            if let Value::Bytes(b) = v {
                match field {
                    1 => {
                        if b.len() > self.key_bytes {
                            return Err(Error::AttributesTooLarge);
                        }
                        key = b;
                    }
                    2 => {
                        if b.len() > self.value_bytes {
                            return Err(Error::AttributesTooLarge);
                        }
                        value = b;
                    }
                    _ => {}
                }
            }
            Ok(())
        })?;
        self.pair(key, value, count, bytes)
    }

    pub fn remote_write(&self, input: &[u8]) -> Result<(), Error> {
        let (mut records, mut groups) = (0, 0);
        fields(input, |field, value| {
            if let (1, Value::Bytes(series)) = (field, value) {
                increment(&mut groups, self.records)?;
                let (mut count, mut bytes) = (0, 0);
                fields(series, |field, value| {
                    match (field, value) {
                        (1, Value::Bytes(pair)) => self.wire_pair(pair, &mut count, &mut bytes)?,
                        (2 | 3 | 4, Value::Bytes(_)) => increment(&mut records, self.records)?,
                        _ => {}
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })
    }

    pub fn loki(&self, input: &[u8]) -> Result<(), Error> {
        let (mut records, mut groups) = (0, 0);
        fields(input, |field, value| {
            if let (1, Value::Bytes(stream)) = (field, value) {
                increment(&mut groups, self.records)?;
                fields(stream, |field, value| {
                    match (field, value) {
                        (1, Value::Bytes(raw)) => {
                            // Go escapes can use eight wire bytes per decoded
                            // byte. Bound parser allocations even for malformed
                            // labels, before last-wins' quadratic deduplication.
                            let raw_cap = self
                                .attribute_bytes
                                .saturating_mul(8)
                                .saturating_add(self.attributes.saturating_mul(8));
                            if raw.len() > raw_cap {
                                return Err(Error::AttributesTooLarge);
                            }
                            if let Some(labels) = labels::parse_limited(raw, self.attributes)
                                .map_err(|_| Error::AttributesTooLarge)?
                            {
                                let (mut count, mut bytes) = (0, 0);
                                for (key, value) in labels {
                                    self.pair(key.as_bytes(), &value, &mut count, &mut bytes)?;
                                }
                            }
                        }
                        (2, Value::Bytes(entry)) => {
                            increment(&mut records, self.records)?;
                            let (mut count, mut bytes) = (0, 0);
                            fields(entry, |field, value| {
                                if let (3, Value::Bytes(pair)) = (field, value) {
                                    self.wire_pair(pair, &mut count, &mut bytes)?;
                                }
                                Ok(())
                            })?;
                        }
                        _ => {}
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::encode;

    fn field(number: u64, value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode::bytes(&mut out, number, value);
        out
    }

    fn limits() -> Limits {
        (2, 2, 4, 4, 8).into()
    }

    #[test]
    fn counts_empty_and_malformed_entries_before_decoding() {
        for (kind, record_field) in [("loki", 2), ("metrics", 2)] {
            let entries = field(record_field, b"").repeat(2);
            let request = field(1, &entries);
            let check = |bytes: &[u8]| {
                if kind == "loki" {
                    limits().loki(bytes)
                } else {
                    limits().remote_write(bytes)
                }
            };
            assert_eq!(check(&request), Ok(()));
            assert_eq!(
                check(&field(1, &field(record_field, b"").repeat(3))),
                Err(Error::TooManyRecords)
            );
            assert_eq!(check(&field(1, b"").repeat(3)), Err(Error::TooManyRecords));
        }
    }

    #[test]
    fn duplicate_pairs_and_bytes_are_bounded() {
        let pair = [field(1, b"a"), field(2, b"123")].concat();
        assert_eq!(
            limits().remote_write(&field(1, &field(1, &pair).repeat(2))),
            Ok(())
        );
        assert_eq!(
            limits().remote_write(&field(1, &field(1, &pair).repeat(3))),
            Err(Error::AttributesTooLarge)
        );
        let big = [field(1, b"a"), field(2, b"12345")].concat();
        assert_eq!(
            limits().remote_write(&field(1, &field(1, &big))),
            Err(Error::AttributesTooLarge)
        );
        let entry = field(3, &pair).repeat(3);
        assert_eq!(
            limits().loki(&field(1, &field(2, &entry))),
            Err(Error::AttributesTooLarge)
        );
    }

    #[test]
    fn loki_labels_are_checked_before_last_wins() {
        assert_eq!(
            limits().loki(&field(1, &field(1, br#"{a="123",a="123"}"#))),
            Ok(())
        );
        assert_eq!(
            limits().loki(&field(1, &field(1, br#"{a="123",a="123",a="123"}"#))),
            Err(Error::AttributesTooLarge)
        );
        assert_eq!(
            limits().loki(&field(1, &field(1, br#"{a="12345"}"#))),
            Err(Error::AttributesTooLarge)
        );
    }

    #[test]
    fn corrupt_wire_does_not_pass_preflight() {
        assert_eq!(limits().loki(&[0x0a, 10]), Err(Error::InvalidProtobuf));
        assert_eq!(
            limits().remote_write(&[0x0a, 10]),
            Err(Error::InvalidProtobuf)
        );
    }
}
