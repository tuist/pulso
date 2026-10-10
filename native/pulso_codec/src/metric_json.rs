//! JSON encoding of `[%Pulso.Record.MetricSample{}]` for MCP responses.
//!
//! The fast path promises semantic parity with the Elixir fallback, not
//! identical key order or float spelling. Unsupported sample shapes and
//! invalid UTF-8 return `Fallback`, leaving error handling to Elixir.
//!
//! Label binaries are borrowed during encoding. Output is built in a
//! `Vec<u8>` and copied once into an Erlang binary at the NIF boundary.

use rustler::{Binary, Encoder, Env, ListIterator, MapIterator, Term};

use crate::atoms;
use crate::json_write::write_str;

#[derive(Debug)]
pub enum Error {
    Fallback,
}

pub fn encode<'a>(env: Env<'a>, samples: Term<'a>) -> Result<Vec<u8>, Error> {
    // Validate the whole list before iteration: an improper tail must not
    // panic in ListIterator or produce a successful partial response.
    let len = samples.list_length().map_err(|_| Error::Fallback)?;
    let items: ListIterator = samples.decode().map_err(|_| Error::Fallback)?;
    // Reserve an estimate, not a limit; larger label sets grow the buffer.
    let mut buf: Vec<u8> = Vec::with_capacity(len.saturating_mul(96).max(64));
    let struct_key = atoms::struct_key().encode(env);
    let expected = atoms::metric_sample().encode(env);
    let series_id = atoms::series_id().encode(env);
    let timestamp_ns = atoms::timestamp_ns().encode(env);
    let value_key = atoms::value().encode(env);
    let labels_key = atoms::labels().encode(env);
    let nil = atoms::nil().encode(env);

    buf.push(b'[');
    let mut first = true;
    // Reuse only the previous label JSON range. Compare complete maps,
    // never series IDs, and keep the cache bounded to one entry.
    let mut previous_labels: Option<(Term<'a>, usize, usize)> = None;
    for sample in items {
        if !first {
            buf.push(b',');
        }
        first = false;

        let tag = sample.map_get(struct_key).map_err(|_| Error::Fallback)?;
        if tag.as_c_arg() != expected.as_c_arg() {
            return Err(Error::Fallback);
        }

        let sid = sample.map_get(series_id).map_err(|_| Error::Fallback)?;
        let ts = sample.map_get(timestamp_ns).map_err(|_| Error::Fallback)?;
        let val = sample.map_get(value_key).map_err(|_| Error::Fallback)?;
        let lbls = sample.map_get(labels_key).map_err(|_| Error::Fallback)?;

        buf.extend_from_slice(b"{\"series_id\":");
        write_int_or_null(&mut buf, sid, nil)?;
        buf.extend_from_slice(b",\"timestamp_ns\":");
        write_int_or_null(&mut buf, ts, nil)?;
        buf.extend_from_slice(b",\"value\":");
        write_float_or_null(&mut buf, val, nil)?;
        buf.extend_from_slice(b",\"labels\":");
        match previous_labels {
            Some((previous, start, end)) if previous == lbls => {
                buf.extend_from_within(start..end);
            }
            _ => {
                let start = buf.len();
                write_labels_map(&mut buf, lbls)?;
                previous_labels = Some((lbls, start, buf.len()));
            }
        }
        buf.push(b'}');
    }
    buf.push(b']');

    Ok(buf)
}

fn write_int_or_null<'a>(buf: &mut Vec<u8>, term: Term<'a>, nil: Term<'a>) -> Result<(), Error> {
    if term.as_c_arg() == nil.as_c_arg() {
        buf.extend_from_slice(b"null");
        return Ok(());
    }
    let v: i64 = term.decode().map_err(|_| Error::Fallback)?;
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(v).as_bytes());
    Ok(())
}

fn write_float_or_null<'a>(buf: &mut Vec<u8>, term: Term<'a>, nil: Term<'a>) -> Result<(), Error> {
    if term.as_c_arg() == nil.as_c_arg() {
        buf.extend_from_slice(b"null");
        return Ok(());
    }
    // Accept both float and integer samples; JSON cannot represent NaN/inf.
    if let Ok(f) = term.decode::<f64>() {
        if !f.is_finite() {
            return Err(Error::Fallback);
        }
        let mut tmp = ryu::Buffer::new();
        buf.extend_from_slice(tmp.format(f).as_bytes());
        return Ok(());
    }
    if let Ok(i) = term.decode::<i64>() {
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(i).as_bytes());
        return Ok(());
    }
    Err(Error::Fallback)
}

fn write_labels_map<'a>(buf: &mut Vec<u8>, term: Term<'a>) -> Result<(), Error> {
    let iter = MapIterator::new(term).ok_or(Error::Fallback)?;
    buf.push(b'{');
    let mut first = true;
    for (k, v) in iter {
        if !first {
            buf.push(b',');
        }
        first = false;
        write_json_binary(buf, k)?;
        buf.push(b':');
        write_json_binary(buf, v)?;
    }
    buf.push(b'}');
    Ok(())
}

fn write_json_binary<'a>(buf: &mut Vec<u8>, term: Term<'a>) -> Result<(), Error> {
    let bin: Binary = term.decode().map_err(|_| Error::Fallback)?;
    simdutf8::basic::from_utf8(bin.as_slice()).map_err(|_| Error::Fallback)?;
    write_str(buf, bin.as_slice());
    Ok(())
}
