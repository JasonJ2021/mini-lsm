#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    read_first_iter: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut read_first_iter = true;
        if !a.is_valid() || (a.is_valid() && b.is_valid() && a.key() > b.key()) {
            read_first_iter = false;
        }
        Ok(TwoMergeIterator {
            a,
            b,
            read_first_iter,
        })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.read_first_iter {
            return self.a.key();
        } else {
            return self.b.key();
        }
    }

    fn value(&self) -> &[u8] {
        if self.read_first_iter {
            return self.a.value();
        } else {
            return self.b.value();
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        while self.read_first_iter && self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }
        if self.read_first_iter {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        if !self.a.is_valid()
            || (self.a.is_valid() && self.b.is_valid() && self.a.key() > self.b.key())
        {
            self.read_first_iter = false;
        } else {
            self.read_first_iter = true;
        }
        Ok(())
    }
}
