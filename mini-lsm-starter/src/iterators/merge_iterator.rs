#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::mem::swap;

use anyhow::{anyhow, Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator {
                iters: BinaryHeap::<HeapWrapper<I>>::new(),
                current: None,
            };
        }
        let mut heap = BinaryHeap::<HeapWrapper<I>>::new();
        for (idx, item) in iters.into_iter().enumerate() {
            if !item.is_valid() {
                continue;
            }
            heap.push(HeapWrapper(idx, item));
        }
        // assert!(!heap.is_empty());
        let current = if heap.is_empty() {
            None
        } else {
            Some(heap.pop().unwrap())
        };

        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        return self.current.as_ref().unwrap().1.key();
    }

    fn value(&self) -> &[u8] {
        return self.current.as_ref().unwrap().1.value();
    }

    fn is_valid(&self) -> bool {
        return self.current.is_some() && self.current.as_ref().unwrap().1.is_valid();
    }

    fn next(&mut self) -> Result<()> {
        // find out first iterator's key not same with current
        let cur_key = self.key().to_key_vec();
        let current = self.current.as_mut().unwrap();
        let mut cur_item_valid = current.1.is_valid();
        while current.1.is_valid() {
            if current.1.next().is_ok()
                && current.1.is_valid()
                && current.1.key() != cur_key.as_key_slice()
            {
                break;
            } else {
                cur_item_valid = false;
            }
        }
        // self.current is:
        // 1. invalid
        // 2. point to a larger key
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            // filter all iter with first key equal to cur_key
            // TODO: this IF seems can be deleted
            if inner_iter.1.is_valid() {
                let top_key = inner_iter.1.key();
                if top_key != cur_key.as_key_slice() {
                    break;
                } else {
                    let ans = inner_iter.1.next();
                    if ans.is_err() {
                        PeekMut::pop(inner_iter);
                        return Err(anyhow!(
                            "Next returns an error due to disk failure, network failure etc."
                        ));
                    }
                    if !inner_iter.1.is_valid() {
                        PeekMut::pop(inner_iter);
                    }
                }
            } else {
                PeekMut::pop(inner_iter);
            }
        }
        let top_item = self.iters.peek_mut();
        if cur_item_valid {
            if let Some(mut top_item) = top_item {
                if current.1.key() > top_item.1.key()
                    || (current.1.key() == top_item.1.key() && current.0 > top_item.0)
                {
                    swap(current, &mut *top_item);
                }
            }
        } else if let Some(top_item) = top_item {
            let new_item = PeekMut::pop(top_item);
            self.current = Some(new_item);
        }
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.iters.len() + self.current.is_some() as usize
    }
}
