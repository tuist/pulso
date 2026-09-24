// Pulso object store NIF.
//
// A thin wrapper around the `object_store` crate exposing S3-compatible
// put/get/delete/list to Elixir. Every entry point runs on a dirty I/O
// scheduler and drives a shared multi-threaded tokio runtime.
//
// The Elixir side (`Pulso.ObjectStore.NIF`) always passes an explicit
// `StoreConfig` map; nothing in this crate reads process environment.

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutPayload};
use once_cell::sync::Lazy;
use rustler::{Atom, Binary, Env, Error, NewBinary, NifResult};
use std::sync::Arc;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for pulso_object_store")
});

mod atoms {
    rustler::atoms! {
        ok,
        error,
        not_found
    }
}

#[derive(rustler::NifMap)]
struct StoreConfig {
    bucket: String,
    endpoint: Option<String>,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    allow_http: bool,
}

fn nif_error<E: std::fmt::Display>(err: E) -> Error {
    Error::Term(Box::new(err.to_string()))
}

// A NotFound response from the object store surfaces as the atom
// `:not_found` on the Elixir side. Everything else stays as a string
// message so the caller keeps the underlying context (permission denied,
// throttling, timeout, etc.).
fn map_object_store_error(err: ObjectStoreError) -> Error {
    match err {
        ObjectStoreError::NotFound { .. } => Error::Term(Box::new(atoms::not_found())),
        other => Error::Term(Box::new(other.to_string())),
    }
}

fn build_store(config: &StoreConfig) -> Result<Arc<dyn ObjectStore>, Error> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(config.allow_http);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    builder
        .build()
        .map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
        .map_err(nif_error)
}

#[rustler::nif(schedule = "DirtyIo")]
fn put(config: StoreConfig, key: String, data: Binary) -> NifResult<Atom> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    // Zero-copy hand-off of the Erlang binary to `object_store`. `Bytes::
    // from_static` normally requires `&'static [u8]`, and we do not have
    // one — the slice's real lifetime is `data`'s NIF-call lifetime. The
    // transmute lies to the compiler about lifetime; the runtime invariant
    // that keeps this sound is:
    //
    //   1. `RUNTIME.block_on(...)` completes before this NIF returns.
    //   2. The `PutPayload` (and therefore every `Bytes` clone inside the
    //      HTTP client) is dropped when the future returned by
    //      `store.put(...)` resolves.
    //   3. The Erlang binary's memory stays valid for the entire duration
    //      of the NIF call (Erlang refcounts the underlying heap; it can
    //      only be released after the NIF returns).
    //
    // As long as we never spawn the S3 request onto a background task or
    // otherwise let `Bytes` outlive `block_on`, no dangling reference
    // reaches the Rust side. The alternative — `Bytes::copy_from_slice` —
    // adds one full-payload memcpy per PUT, which showed up on the hot
    // path once ingest throughput started climbing.
    let slice: &[u8] = data.as_slice();
    let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
    let payload: PutPayload = Bytes::from_static(static_slice).into();

    RUNTIME
        .block_on(store.put(&path, payload))
        .map_err(nif_error)?;

    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn get<'a>(env: Env<'a>, config: StoreConfig, key: String) -> NifResult<(Atom, Binary<'a>)> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    // Get the object metadata first so we know the target binary size.
    let obj = RUNTIME
        .block_on(store.get(&path))
        .map_err(map_object_store_error)?;
    let size = obj.meta.size as usize;

    // Allocate the destination binary on the Erlang heap (via `NewBinary`)
    // and stream the object body directly into it. The alternative —
    // `GetResult::bytes()` — first collects every chunk into an
    // intermediate `BytesMut` on the Rust heap; cutting that saves one
    // full-payload allocation and copy at the boundary.
    let mut new_binary = NewBinary::new(env, size);
    let dst = new_binary.as_mut_slice();
    let mut offset: usize = 0;

    RUNTIME
        .block_on(async {
            let mut stream = obj.into_stream();
            while let Some(chunk) = stream.try_next().await? {
                let end = offset + chunk.len();
                if end > size {
                    return Err(ObjectStoreError::Generic {
                        store: "pulso_object_store",
                        source: format!("body exceeded declared size ({} > {})", end, size).into(),
                    });
                }
                dst[offset..end].copy_from_slice(&chunk);
                offset = end;
            }
            if offset != size {
                return Err(ObjectStoreError::Generic {
                    store: "pulso_object_store",
                    source: format!("body shorter than declared size ({} < {})", offset, size).into(),
                });
            }
            Ok(())
        })
        .map_err(map_object_store_error)?;

    Ok((atoms::ok(), Binary::from(new_binary)))
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete(config: StoreConfig, key: String) -> NifResult<Atom> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    RUNTIME
        .block_on(async { store.delete(&path).await })
        .map_err(map_object_store_error)?;

    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn list(config: StoreConfig, prefix: String) -> NifResult<(Atom, Vec<String>)> {
    let store = build_store(&config)?;
    let path = if prefix.is_empty() {
        None
    } else {
        Some(Path::from(prefix))
    };

    let keys = RUNTIME
        .block_on(async {
            let mut stream = store.list(path.as_ref());
            let mut acc = Vec::new();
            while let Some(meta) = stream.try_next().await? {
                acc.push(meta.location.to_string());
            }
            Ok::<Vec<String>, object_store::Error>(acc)
        })
        .map_err(nif_error)?;

    Ok((atoms::ok(), keys))
}

rustler::init!("Elixir.Pulso.ObjectStore.NIF");
