//! Zero-copy `Bytes` over an Erlang binary.
//!
//! Consumers such as the Parquet reader and reqwest's retry layer need a
//! `'static` input they can clone. Saving the binary into a
//! process-independent environment owned by the `Bytes` keeps it alive
//! for as long as any clone exists, past the NIF call.
//!
//! Kept identical in `pulso_codec` and `pulso_object_store`.

use bytes::Bytes;
use rustler::env::SavedTerm;
use rustler::{Binary, OwnedEnv};
use std::ptr::NonNull;

/// An Erlang binary kept alive by its own environment, for use as a
/// `Bytes` owner.
struct ErlangBackedBytes {
    /// Handle to the saved binary.
    _saved: SavedTerm,
    /// Holds the binary: a refcount for off-heap binaries, a copy for
    /// small heap ones. Either way the bytes never move.
    _env: OwnedEnv,
    /// The bytes inside `_env`. A raw pointer, since a reference cannot
    /// borrow from a sibling field.
    data: NonNull<[u8]>,
}

// The bytes are immutable and kept alive by `_env`, which is itself
// `Send`; freeing a process-independent environment is allowed from any
// thread.
unsafe impl Send for ErlangBackedBytes {}

impl AsRef<[u8]> for ErlangBackedBytes {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `data` points at the binary saved in `_env`, which
        // lives as long as `self` and never mutates or moves the data.
        unsafe { self.data.as_ref() }
    }
}

/// Wrap `binary` in a `Bytes` without copying it. Every clone keeps the
/// binary alive past the NIF call.
pub fn from_binary(binary: Binary) -> Bytes {
    let owned_env = OwnedEnv::new();
    let saved = owned_env.save(binary);
    let data = owned_env.run(|e| {
        let b: Binary = saved.load(e).decode().expect("saved term is a binary");
        NonNull::from(b.as_slice())
    });
    Bytes::from_owner(ErlangBackedBytes {
        _saved: saved,
        _env: owned_env,
        data,
    })
}
