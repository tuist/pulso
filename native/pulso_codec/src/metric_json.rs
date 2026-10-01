//! JSON encode of `[%Pulso.Record.MetricSample{}]` for the MCP
//! `query_metrics` response.
//!
//! Byte-for-byte parity with the Elixir fallback is **not** promised —
//! only semantic parity (same keys, same values, decodable by any JSON
//! parser). We fix the key order (`series_id`, `timestamp_ns`,
//! `value`, `labels`) because it is cheap in Rust and worth it for
//! reader stability; Elixir's `JSON.encode!` on a plain map does not.
//!
//! Fallback discipline: any sample term that is not a well-shaped
//! `%MetricSample{}` returns `:fallback` to the NIF, which the Elixir
//! caller downgrades to the pure-Elixir encoder. The caller never
//! loses the record — same contract as `Pulso.JSON`.
//!
//! Zero copies on the output: writes straight into a `Vec<u8>` that
//! becomes an Erlang binary at the NIF boundary (see `lib.rs`
//! `copy()`), and reads label name/value binaries by slice reference
//! so no intermediate `String` or `Vec<u8>` is built per field.

use rustler::{Binary, Encoder, Env, ListIterator, MapIterator, Term};

use crate::atoms;

#[derive(Debug)]
pub enum Error {
    Fallback,
}

pub fn encode<'a>(env: Env<'a>, samples: Term<'a>) -> Result<Vec<u8>, Error> {
    let items: ListIterator = samples.decode().map_err(|_| Error::Fallback)?;
    // 48 bytes per sample is a comfortable median for 4-label series —
    // see the bench fixture. The Vec grows on undershoot; overshoot is
    // cheap.
    let hint = samples.list_length().unwrap_or(0).saturating_mul(96);
    let mut buf: Vec<u8> = Vec::with_capacity(hint.max(64));
    let struct_key = atoms::struct_key().encode(env);
    let expected = atoms::metric_sample().encode(env);
    let series_id = atoms::series_id().encode(env);
    let timestamp_ns = atoms::timestamp_ns().encode(env);
    let value_key = atoms::value().encode(env);
    let labels_key = atoms::labels().encode(env);
    let nil = atoms::nil().encode(env);

    buf.push(b'[');
    let mut first = true;
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
        write_labels_map(&mut buf, lbls)?;
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
    // Accept both float and integer value terms. The former is what
    // Prometheus emits; the latter is possible when a producer stored
    // an integer sample.
    if let Ok(f) = term.decode::<f64>() {
        if !f.is_finite() {
            // JSON has no NaN/inf. Fall back so the Elixir side can
            // decide (currently `JSON.encode!` will also fail; this
            // just preserves the behavior).
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
    write_json_string(buf, bin.as_slice());
    Ok(())
}

fn write_json_string(buf: &mut Vec<u8>, s: &[u8]) {
    buf.push(b'"');
    for &b in s {
        match b {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            0x08 => buf.extend_from_slice(b"\\b"),
            0x09 => buf.extend_from_slice(b"\\t"),
            0x0a => buf.extend_from_slice(b"\\n"),
            0x0c => buf.extend_from_slice(b"\\f"),
            0x0d => buf.extend_from_slice(b"\\r"),
            b if b < 0x20 => {
                buf.extend_from_slice(b"\\u00");
                buf.push(hex_nibble(b >> 4));
                buf.push(hex_nibble(b & 0xf));
            }
            b => buf.push(b),
        }
    }
    buf.push(b'"');
}

fn hex_nibble(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        _ => b'a' + (n - 10),
    }
}
