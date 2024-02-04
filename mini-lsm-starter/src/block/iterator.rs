#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut blk_iter = BlockIterator::new(block);
        blk_iter.seek_to_first();
        blk_iter.first_key = blk_iter.key.clone();
        blk_iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut blk_iter = BlockIterator::create_and_seek_to_first(block);
        while blk_iter.is_valid() && blk_iter.key() < key {
            blk_iter.next();
        }
        blk_iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        return self.key.as_key_slice();
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        return &self.block.data[self.value_range.0..self.value_range.1];
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        return !self.key.is_empty();
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        let offset = self.block.offsets[0] as usize;
        let mut ptr = &self.block.data[offset..];
        let key_len = ptr.get_u16();
        self.key
            .set_from_slice(KeySlice::from_slice(&ptr[0..key_len as usize]));
        ptr.advance(key_len as usize);
        let value_len = ptr.get_u16();
        self.value_range = (
            offset + 4 + key_len as usize,
            offset + 4 + key_len as usize + value_len as usize,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() - 1 {
            // End of iterator
            self.key = KeyVec::new();
            return;
        }
        self.idx += 1;
        let offset = self.block.offsets[self.idx] as usize;
        let mut ptr = &self.block.data[offset..];
        let key_len = ptr.get_u16();
        self.key
            .set_from_slice(KeySlice::from_slice(&ptr[0..key_len as usize]));
        ptr.advance(key_len as usize);
        let value_len = ptr.get_u16();
        self.value_range = (
            offset + 4 + key_len as usize,
            offset + 4 + key_len as usize + value_len as usize,
        );
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key() < key {
            self.next();
        }
    }
}
