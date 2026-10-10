// Pulso object store NIF.
//
// A thin wrapper around the `object_store` crate exposing S3-compatible
// put/get/delete/list — plus the conditional variants Pulso's manifest CAS
// relies on — to Elixir. Every entry point runs on a dirty I/O scheduler
// and drives a shared multi-threaded tokio runtime.
//
// The Elixir side (`Pulso.ObjectStore.NIF`) always passes an explicit
// `StoreConfig` map; nothing in this crate reads process environment.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use once_cell::sync::Lazy;
use rustler::env::SavedTerm;
use rustler::{Atom, Binary, Env, Error, NewBinary, NifResult, OwnedEnv};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
        not_found,
        not_modified,
        already_exists,
        precondition_failed,
        response_too_large,
        retention_overloaded
    }
}

// Boot-time ABI fence: older release artifacts must fail before serving ingest.
#[rustler::nif]
fn retention_api_version() -> u32 {
    1
}

#[derive(rustler::NifMap, Clone, PartialEq, Eq, Hash)]
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
        ObjectStoreError::NotModified { .. } => Error::Term(Box::new(atoms::not_modified())),
        ObjectStoreError::AlreadyExists { .. } => Error::Term(Box::new(atoms::already_exists())),
        ObjectStoreError::Precondition { .. } => {
            Error::Term(Box::new(atoms::precondition_failed()))
        }
        other => Error::Term(Box::new(other.to_string())),
    }
}

// One client per distinct config, reused across calls. Building a store
// per call created a fresh HTTP connection pool every time, so every PUT
// and GET paid a new TCP (and TLS) handshake; a cached store keeps
// connections alive. A config change (e.g. rotated credentials) is a new
// key and gets its own client.
static STORES: Lazy<Mutex<HashMap<StoreConfig, Arc<dyn ObjectStore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn build_store(config: &StoreConfig) -> Result<Arc<dyn ObjectStore>, Error> {
    let mut stores = STORES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(store) = stores.get(config) {
        return Ok(Arc::clone(store));
    }

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(config.allow_http);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    let store = builder
        .build()
        .map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
        .map_err(nif_error)?;
    stores.insert(config.clone(), Arc::clone(&store));
    Ok(store)
}

// Hand the Erlang binary to `object_store` without copying it. The
// binary is saved into a process-independent environment, which holds a
// reference to it (a refcount, not a copy, for off-heap binaries), and
// the resulting `Bytes` owns that environment. reqwest may clone the body
// for retries and keep clones past the NIF call; every clone shares the
// owner, so the Erlang binary stays alive until the last one is dropped.
// That is what makes this sound where a lifetime-extended slice was not.
struct ErlangBinary {
    _saved: SavedTerm,
    _env: OwnedEnv,
    ptr: *const u8,
    len: usize,
}

// The bytes are immutable and kept alive by `_env`, which is itself
// `Send`; freeing a process-independent environment is allowed from any
// thread.
unsafe impl Send for ErlangBinary {}

impl AsRef<[u8]> for ErlangBinary {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `ptr`/`len` describe the binary saved in `_env`, which
        // lives as long as `self` and never mutates or moves the data.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn payload_from_binary(data: Binary) -> PutPayload {
    let env = OwnedEnv::new();
    let saved = env.save(data);
    let (ptr, len) = env.run(|e| {
        let b: Binary = saved.load(e).decode().expect("saved term is a binary");
        (b.as_slice().as_ptr(), b.len())
    });
    Bytes::from_owner(ErlangBinary {
        _saved: saved,
        _env: env,
        ptr,
        len,
    })
    .into()
}

// The `object_store` crate returns `PutResult { e_tag, version }`. S3
// always supplies an ETag; treat its absence as a hard error rather than
// silently returning an empty string — a CAS caller that trusts a bogus
// ETag would spin forever on `precondition_failed`.
fn etag_or_error(e_tag: Option<String>) -> Result<String, Error> {
    e_tag.ok_or_else(|| nif_error("object store returned no ETag"))
}

#[rustler::nif(schedule = "DirtyIo")]
fn put(config: StoreConfig, key: String, data: Binary) -> NifResult<(Atom, String)> {
    let store = build_store(&config)?;
    let path = Path::from(key);
    let payload = payload_from_binary(data);

    let result = RUNTIME
        .block_on(store.put(&path, payload))
        .map_err(map_object_store_error)?;

    Ok((atoms::ok(), etag_or_error(result.e_tag)?))
}

// Conditional create: If-None-Match: *. Fails with `:already_exists` if
// the key already has an object. This is how the manifest is first
// written — races between two nodes trying to create the same manifest
// resolve deterministically: exactly one wins, the loser reloads.
#[rustler::nif(schedule = "DirtyIo")]
fn put_if_none_match(config: StoreConfig, key: String, data: Binary) -> NifResult<(Atom, String)> {
    let store = build_store(&config)?;
    let path = Path::from(key);
    let payload = payload_from_binary(data);
    let opts = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };

    let result = RUNTIME
        .block_on(store.put_opts(&path, payload, opts))
        .map_err(map_object_store_error)?;

    Ok((atoms::ok(), etag_or_error(result.e_tag)?))
}

// Conditional update: If-Match: <etag>. Fails with `:precondition_failed`
// if the current object's ETag differs from the caller's expected one.
// This is the manifest CAS primitive: every legitimate update carries the
// ETag it read; a stale writer loses to a fresher one.
#[rustler::nif(schedule = "DirtyIo")]
fn put_if_match(
    config: StoreConfig,
    key: String,
    data: Binary,
    etag: String,
) -> NifResult<(Atom, String)> {
    let store = build_store(&config)?;
    let path = Path::from(key);
    let payload = payload_from_binary(data);
    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some(etag),
            version: None,
        }),
        ..PutOptions::default()
    };

    let result = RUNTIME
        .block_on(store.put_opts(&path, payload, opts))
        .map_err(map_object_store_error)?;

    Ok((atoms::ok(), etag_or_error(result.e_tag)?))
}

// Streams a GET body into a Rustler `NewBinary` allocated on the Erlang
// heap — one full-payload allocation, zero intermediate Rust-side copies.
// The alternative `GetResult::bytes()` first collects every chunk into a
// `BytesMut` on the Rust heap, then hands us a `Bytes` we would still
// have to copy into a `NewBinary`; two full-payload allocations instead
// of one.
fn stream_body_into<'a>(
    env: Env<'a>,
    obj: object_store::GetResult,
) -> Result<(String, Binary<'a>), Error> {
    let size = obj.meta.size as usize;
    let e_tag = etag_or_error(obj.meta.e_tag.clone())?;

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
                    source: format!("body shorter than declared size ({} < {})", offset, size)
                        .into(),
                });
            }
            Ok(())
        })
        .map_err(map_object_store_error)?;

    Ok((e_tag, Binary::from(new_binary)))
}

#[rustler::nif(schedule = "DirtyIo")]
fn get<'a>(env: Env<'a>, config: StoreConfig, key: String) -> NifResult<(Atom, Binary<'a>)> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    let obj = RUNTIME
        .block_on(store.get(&path))
        .map_err(map_object_store_error)?;

    let (_etag, binary) = stream_body_into(env, obj)?;
    Ok((atoms::ok(), binary))
}

// Conditional GET: if `etag` is non-empty, sends `If-None-Match: <etag>`.
// - Object unchanged → `:not_modified` (no body transferred). This is how
//   the query path avoids re-downloading a manifest every time.
// - Object changed or no etag was supplied → `{:ok, new_etag, body}`.
// - Missing key → `{:error, :not_found}`.
//
// The empty string sentinel avoids allocating an option on the Elixir
// side just to say "no cached ETag."
#[rustler::nif(schedule = "DirtyIo")]
fn get_if_none_match<'a>(
    env: Env<'a>,
    config: StoreConfig,
    key: String,
    etag: String,
) -> NifResult<(Atom, String, Binary<'a>)> {
    let store = build_store(&config)?;
    let path = Path::from(key);

    let if_none_match = if etag.is_empty() { None } else { Some(etag) };
    let opts = GetOptions {
        if_none_match,
        ..GetOptions::default()
    };

    // 304 comes back as `ObjectStoreError::NotModified`, mapped to
    // `:not_modified`. Callers pattern-match on that and skip parsing.
    let obj = RUNTIME
        .block_on(store.get_opts(&path, opts))
        .map_err(map_object_store_error)?;

    let (e_tag, binary) = stream_body_into(env, obj)?;
    Ok((atoms::ok(), e_tag, binary))
}

// Reject oversized metadata before allocating its body on the Erlang heap.
#[rustler::nif(schedule = "DirtyIo")]
fn get_bounded<'a>(
    env: Env<'a>,
    config: StoreConfig,
    key: String,
    etag: String,
    limit: u64,
) -> NifResult<(Atom, String, Binary<'a>)> {
    let store = build_store(&config)?;
    let opts = GetOptions {
        if_none_match: if etag.is_empty() { None } else { Some(etag) },
        ..GetOptions::default()
    };
    let obj = RUNTIME
        .block_on(store.get_opts(&Path::from(key), opts))
        .map_err(map_object_store_error)?;
    if obj.meta.size > limit {
        return Err(Error::Term(Box::new(atoms::response_too_large())));
    }
    let (etag, body) = stream_body_into(env, obj)?;
    Ok((atoms::ok(), etag, body))
}

// A lexical start-after cursor remains valid when earlier objects are deleted.
// Never collect the whole provider stream; a call returns at most 1,000 keys.
// Jump over each tenant's entire subtree instead of walking its telemetry.
// The next ASCII character after '/' is '0'. With Pulso's tenant alphabet,
// `<tenant>0` sorts past `<tenant>/...` but before every subsequent tenant's
// keys, including `<tenant>0/...`. Each provider request has a bounded page.
#[rustler::nif(schedule = "DirtyIo")]
fn discover_tenants(
    config: StoreConfig,
    after: String,
    limit: usize,
) -> NifResult<(Atom, Vec<String>, Option<String>)> {
    if limit == 0 || limit > 64 {
        return Err(nif_error("invalid discovery limit"));
    }
    let store = build_store(&config)?;
    let prefix = Path::from("tenants/");
    let mut cursor = if after.is_empty() { None } else { Some(after) };
    let mut tenants = Vec::new();
    for _ in 0..limit {
        let offset = cursor.as_ref().map(|t| Path::from(format!("tenants/{t}0")));
        let mut stream = match &offset {
            Some(path) => store.list_with_offset(Some(&prefix), path),
            None => store.list(Some(&prefix)),
        };
        let object = RUNTIME
            .block_on(stream.try_next())
            .map_err(map_object_store_error)?;
        let Some(object) = object else {
            return Ok((atoms::ok(), tenants, None));
        };
        let key = object.location.to_string();
        let tenant = key
            .strip_prefix("tenants/")
            .and_then(|k| k.split('/').next())
            .ok_or_else(|| nif_error("invalid tenant listing"))?
            .to_string();
        if tenant.is_empty() {
            return Err(nif_error("invalid tenant listing"));
        }
        cursor = Some(tenant.clone());
        if tenant.len() <= 128
            && tenant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        {
            tenants.push(tenant);
        }
    }
    Ok((atoms::ok(), tenants, cursor))
}

#[rustler::nif(schedule = "DirtyIo")]
fn list_page(
    config: StoreConfig,
    prefix: String,
    after: String,
    limit: usize,
) -> NifResult<(Atom, Vec<String>, Option<String>)> {
    if limit == 0 || limit > 1000 {
        return Err(nif_error("invalid list page limit"));
    }
    let store = build_store(&config)?;
    let path = Path::from(prefix);
    let offset = Path::from(after);
    let stream = if offset.as_ref().is_empty() {
        store.list(Some(&path))
    } else {
        store.list_with_offset(Some(&path), &offset)
    };
    let objects: Vec<_> = RUNTIME
        .block_on(stream.take(limit + 1).try_collect())
        .map_err(map_object_store_error)?;
    let more = objects.len() > limit;
    let keys: Vec<String> = objects
        .into_iter()
        .take(limit)
        .map(|obj| obj.location.to_string())
        .collect();
    let cursor = if more { keys.last().cloned() } else { None };
    Ok((atoms::ok(), keys, cursor))
}

// Native slots outlive a killed BEAM caller: a DOWN message cannot release an
// I/O operation that is still running on a dirty scheduler / Tokio runtime.
static RETENTION_DELETES: AtomicUsize = AtomicUsize::new(0);
struct DeleteSlot;
impl Drop for DeleteSlot {
    fn drop(&mut self) {
        RETENTION_DELETES.fetch_sub(1, Ordering::SeqCst);
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete_bounded(config: StoreConfig, key: String) -> NifResult<Atom> {
    RETENTION_DELETES
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            if n < 4 {
                Some(n + 1)
            } else {
                None
            }
        })
        .map_err(|_| Error::Term(Box::new(atoms::retention_overloaded())))?;
    let _slot = DeleteSlot;
    let store = build_store(&config)?;
    RUNTIME
        .block_on(store.delete(&Path::from(key)))
        .map_err(map_object_store_error)?;
    Ok(atoms::ok())
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

// Delimiter listing discovers tenant directories without downloading every
// segment key. The object_store client follows provider pagination internally.
#[rustler::nif(schedule = "DirtyIo")]
fn list_prefixes(config: StoreConfig, prefix: String) -> NifResult<(Atom, Vec<String>)> {
    let store = build_store(&config)?;
    let path = Path::from(prefix);
    let result = RUNTIME
        .block_on(store.list_with_delimiter(Some(&path)))
        .map_err(map_object_store_error)?;
    let prefixes = result
        .common_prefixes
        .into_iter()
        .map(|path| path.to_string())
        .collect();
    Ok((atoms::ok(), prefixes))
}

rustler::init!("Elixir.Pulso.ObjectStore.NIF");
