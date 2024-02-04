#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut blk = self.data.clone();
        for &offset in &self.offsets {
            blk.put_u16(offset);
        }
        blk.put_u16(self.offsets.len() as u16);
        return Bytes::from(blk);
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut num_of_elements_ptr = &data[data.len() - 2..];
        let num_of_elements = num_of_elements_ptr.get_u16();
        let mut offsets_ptr = &data[(data.len() - 2 * (num_of_elements as usize + 1))..];
        let data_sec = Vec::from(&data[0..(data.len() - 2 * (num_of_elements as usize + 1))]);
        let mut offsets_sec = Vec::<u16>::new();
        for _ in 0..num_of_elements {
            offsets_sec.push(offsets_ptr.get_u16());
        }
        return Block {
            data: data_sec,
            offsets: offsets_sec,
        };
    }
}
