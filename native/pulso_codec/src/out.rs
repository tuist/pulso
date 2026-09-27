//! Output buffers for the encoders.
//!
//! `BinSink` writes straight into an Erlang-allocated binary and grows it
//! in place, so a finished encoding is handed to the VM without a final
//! copy. `Vec<u8>` implements the same trait for pure-Rust tests.

use rustler::{Binary, Env, OwnedBinary};

pub trait Sink {
    fn extend(&mut self, bytes: &[u8]);
    fn push(&mut self, byte: u8);
    fn len(&self) -> usize;
    /// Append a copy of an earlier range of this output.
    fn repeat(&mut self, start: usize, end: usize);
}

impl Sink for Vec<u8> {
    fn extend(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn repeat(&mut self, start: usize, end: usize) {
        self.extend_from_within(start..end);
    }
}

pub struct BinSink {
    bin: OwnedBinary,
    len: usize,
}

impl BinSink {
    pub fn with_capacity(capacity: usize) -> Self {
        let bin = OwnedBinary::new(capacity.max(64)).expect("allocating an Erlang binary failed");
        BinSink { bin, len: 0 }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        let needed = self.len + additional;
        let capacity = self.bin.as_slice().len();
        if needed > capacity {
            let grown = needed.max(capacity * 2);
            // In place when the allocator can extend the block; otherwise
            // Erlang copies it, amortized like a Vec.
            if !self.bin.realloc(grown) {
                self.bin.realloc_or_copy(grown);
            }
        }
    }

    pub fn finish(mut self, env: Env) -> Binary {
        if self.len != self.bin.as_slice().len() {
            self.bin.realloc_or_copy(self.len);
        }
        self.bin.release(env)
    }
}

impl Sink for BinSink {
    #[inline]
    fn extend(&mut self, bytes: &[u8]) {
        self.reserve(bytes.len());
        self.bin.as_mut_slice()[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
    #[inline]
    fn push(&mut self, byte: u8) {
        self.reserve(1);
        self.bin.as_mut_slice()[self.len] = byte;
        self.len += 1;
    }
    fn len(&self) -> usize {
        self.len
    }
    fn repeat(&mut self, start: usize, end: usize) {
        let n = end - start;
        self.reserve(n);
        self.bin.as_mut_slice().copy_within(start..end, self.len);
        self.len += n;
    }
}
