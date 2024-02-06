#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::{BufMut, Bytes};

use crate::key::{KeyBytes, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// Prev entry size in the data section
    prev_entry_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
            prev_entry_size: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
            self.data.put_u16(key_len);
            self.data.put_slice(key.raw_ref());
            self.data.put_u16(value_len);
            self.data.put_slice(value);
            self.offsets.push(0);
            self.prev_entry_size = key_len as usize + value_len as usize + 4;
            return true;
        }
        if key_len as usize + value_len as usize + 6 + self.cur_blk_size() > self.block_size {
            return false;
        }
        self.data.put_u16(key_len);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value_len);
        self.data.put_slice(value);
        self.offsets
            .push(self.offsets.last().unwrap() + self.prev_entry_size as u16);
        self.prev_entry_size = key_len as usize + value_len as usize + 4;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
    pub fn cur_blk_size(&self) -> usize {
        // data + offsets + num_of_elements(2 bytes)
        self.data.len() + self.offsets.len() * 2 + std::mem::size_of::<u16>()
    }
    pub fn get_first_key(&self) -> KeyBytes {
        return KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.raw_ref()));
    }
}
