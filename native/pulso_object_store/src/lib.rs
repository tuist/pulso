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
use rustler::{Atom, Binary, Env, Error, NifResult, OwnedBinary};
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
    let payload: PutPayload = Bytes::copy_from_slice(data.as_slice()).into();

    RUNTIME
        .block_on(async { store.put(&path, payload).await })
        .map_err(nif_error)?;

    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn get<'a>(env: Env<'a>, config: StoreConfig, key: String) -> NifResult<(Atom, Binary<'a>)> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    let bytes = RUNTIME
        .block_on(async {
            let obj = store.get(&path).await?;
            obj.bytes().await
        })
        .map_err(map_object_store_error)?;

    let mut owned = OwnedBinary::new(bytes.len())
        .ok_or_else(|| Error::Term(Box::new("failed to allocate binary")))?;
    owned.as_mut_slice().copy_from_slice(&bytes);

    Ok((atoms::ok(), Binary::from_owned(owned, env)))
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
