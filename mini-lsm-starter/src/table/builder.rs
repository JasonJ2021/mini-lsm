#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Ok, Result};
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::{
    block::{Block, BlockBuilder},
    key::{Key, KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size: block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            // current block is full
            // stack current block into self.data and store its corresponding metadata
            let old_block_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let first_key = old_block_builder.get_first_key();
            let block = old_block_builder.build();

            let block_metadata = BlockMeta {
                offset: self.data.len(),
                first_key,
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            };
            let block_encoded = block.encode();
            self.data.append(&mut block_encoded.to_vec());
            self.meta.push(block_metadata);
            // Now we should add key, value into new blockbuilder
            self.add(key, value);
        } else {
            // current block is not full
            // TODO: first_key and last_key update can be removed ?
            if self.first_key.is_empty() {
                self.first_key.extend_from_slice(key.raw_ref());
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(key.raw_ref());
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        return self.data.len();
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // 1. Build the current block, prepare block metadata
        let first_key = self.builder.get_first_key();
        let block = self.builder.build();
        let block_metadata = BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        };
        let block_encoded = block.encode();
        self.data.append(&mut block_encoded.to_vec());
        self.meta.push(block_metadata);
        // 2. Save block metadata into self.data
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(u32::try_from(block_meta_offset).unwrap());
        // 3. Flush blocks & index into disk
        let file = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
