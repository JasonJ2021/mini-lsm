#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use core::panic;
use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;
use nom::AsBytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn get_bound_inner(bound: Bound<&[u8]>) -> Bytes {
    match bound {
        Bound::Included(x) => Bytes::copy_from_slice(x),
        Bound::Excluded(x) => Bytes::copy_from_slice(x),
        Bound::Unbounded => Bytes::new(),
    }
}

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        // Remove all delete (key, value) pair in iter
        // If iterator is been tainted in this process, just throw an error
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(Self {
            inner: iter,
            end_bound,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        return self.inner.is_valid() && {
            match &self.end_bound {
                Bound::Unbounded => true,
                Bound::Excluded(end_bound) => self.key() < end_bound.as_bytes(),
                Bound::Included(end_bound) => self.key() <= end_bound.as_bytes(),
            }
        };
    }

    fn key(&self) -> &[u8] {
        return self.inner.key().raw_ref();
    }

    fn value(&self) -> &[u8] {
        return self.inner.value();
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_error: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let has_error = false;
        Self { iter, has_error }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_error && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_error || !self.iter.is_valid() {
            panic!("Indending to access an invalid iterator");
        }
        return self.iter.key();
    }

    fn value(&self) -> &[u8] {
        if self.has_error || !self.iter.is_valid() {
            panic!("Indending to access an invalid iterator");
        }
        return self.iter.value();
    }

    fn next(&mut self) -> Result<()> {
        if self.has_error {
            bail!("Indending to call next on a tainted iteraotr");
        }
        if self.iter.is_valid() {
            if let Err(err) = self.iter.next() {
                self.has_error = true;
                return Err(err);
            }
        }
        // According to the tests, call next on a invalid iterator is not harmful
        Ok(())
    }
}
