// Pulso ingest NIF.
//
// Decodes high-volume ingest wire formats into Elixir terms. The request
// body is decompressed once, straight into an Erlang binary; every string
// in the returned records (lines, label names and values, structured
// metadata) is a sub-binary of that buffer rather than a copy. The only
// other allocations are the terms themselves and label values that
// contained Go escapes.
//
// Because records reference the decompressed buffer, holding a single
// record keeps the whole buffer alive. Callers that retain records past
// the request should `:binary.copy/1` the fields they keep.

mod labels;
mod loki;
mod wire;

use rustler::{Binary, Encoder, Env, NewBinary, NifResult, Term};
use std::borrow::Cow;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        invalid_snappy,
        invalid_protobuf,
        payload_too_large,
        struct_key = "__struct__",
        log = "Elixir.Pulso.Record.Log",
        timestamp_ns,
        observed_timestamp_ns,
        severity_number,
        severity_text,
        service,
        body,
        trace_id,
        span_id,
        attributes,
        resource,
    }
}

/// Decode a Snappy-compressed (raw block format) Loki `PushRequest`.
///
/// The uncompressed size is read from the Snappy header and checked
/// against `max_decompressed` before any output buffer is allocated.
/// Runs on a dirty CPU scheduler: a 1 MiB batch takes a few ms, well
/// past the ~1 ms budget of a regular scheduler.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_loki_push<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
) -> NifResult<Term<'a>> {
    let input = compressed.as_slice();
    let len = match snap::raw::decompress_len(input) {
        Ok(len) => len,
        Err(_) => return Ok(error(env, atoms::invalid_snappy())),
    };
    if len > max_decompressed {
        return Ok(error(env, atoms::payload_too_large()));
    }

    let mut buffer = NewBinary::new(env, len);
    match snap::raw::Decoder::new().decompress(input, buffer.as_mut_slice()) {
        Ok(written) if written == len => {}
        _ => return Ok(error(env, atoms::invalid_snappy())),
    }
    let buffer: Binary = buffer.into();

    match loki::decode(buffer.as_slice()) {
        Some(decoded) => to_terms(env, &buffer, &decoded),
        None => Ok(error(env, atoms::invalid_protobuf())),
    }
}

fn error<'a>(env: Env<'a>, reason: rustler::Atom) -> Term<'a> {
    (atoms::error(), reason).encode(env)
}

fn to_terms<'a>(env: Env<'a>, buffer: &Binary<'a>, decoded: &loki::Decoded) -> NifResult<Term<'a>> {
    let nil = atoms::nil().encode(env);
    let keys = [
        atoms::struct_key(),
        atoms::timestamp_ns(),
        atoms::observed_timestamp_ns(),
        atoms::severity_number(),
        atoms::severity_text(),
        atoms::service(),
        atoms::body(),
        atoms::trace_id(),
        atoms::span_id(),
        atoms::attributes(),
        atoms::resource(),
    ]
    .map(|atom| atom.encode(env));
    let struct_name = atoms::log().encode(env);

    let total = decoded.streams.iter().map(|s| s.records.len()).sum();
    let mut records = Vec::with_capacity(total);

    for stream in &decoded.streams {
        let mut names = Vec::with_capacity(stream.labels.len());
        let mut values = Vec::with_capacity(stream.labels.len());
        for (name, value) in &stream.labels {
            names.push(slice(env, buffer, name.as_bytes())?);
            values.push(match value {
                Cow::Borrowed(b) => slice(env, buffer, b)?,
                Cow::Owned(o) => copy(env, o),
            });
        }
        // One resource map per stream, shared by every record in it.
        let resource = Term::map_from_arrays(env, &names, &values)?;
        let service = stream.service().map_or(nil, |i| values[i]);
        let level = stream.level().map_or(nil, |i| values[i]);

        for record in &stream.records {
            let mut attr_keys = Vec::with_capacity(record.attributes.len());
            let mut attr_values = Vec::with_capacity(record.attributes.len());
            for (k, v) in &record.attributes {
                attr_keys.push(slice(env, buffer, k)?);
                attr_values.push(slice(env, buffer, v)?);
            }
            let fields = [
                struct_name,
                record.timestamp_ns.encode(env),
                nil,
                nil,
                level,
                service,
                slice(env, buffer, record.line)?,
                optional(env, buffer, record.trace_id)?,
                optional(env, buffer, record.span_id)?,
                Term::map_from_arrays(env, &attr_keys, &attr_values)?,
                resource,
            ];
            records.push(Term::map_from_arrays(env, &keys, &fields)?);
        }
    }

    Ok((atoms::ok(), records, decoded.rejected).encode(env))
}

/// A sub-binary of `buffer` when `part` lies inside it; otherwise (empty
/// proto3 defaults point at static memory) a fresh binary.
fn slice<'a>(env: Env<'a>, buffer: &Binary<'a>, part: &[u8]) -> NifResult<Term<'a>> {
    let base = buffer.as_slice();
    let start = (part.as_ptr() as usize).wrapping_sub(base.as_ptr() as usize);
    if !part.is_empty() && start <= base.len() && part.len() <= base.len() - start {
        Ok(buffer.make_subbinary(start, part.len())?.encode(env))
    } else {
        Ok(copy(env, part))
    }
}

fn optional<'a>(env: Env<'a>, buffer: &Binary<'a>, part: Option<&[u8]>) -> NifResult<Term<'a>> {
    match part {
        Some(p) => slice(env, buffer, p),
        None => Ok(atoms::nil().encode(env)),
    }
}

fn copy<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut out = NewBinary::new(env, bytes.len());
    out.as_mut_slice().copy_from_slice(bytes);
    Binary::from(out).encode(env)
}

rustler::init!("Elixir.Pulso.Ingest.NIF");
