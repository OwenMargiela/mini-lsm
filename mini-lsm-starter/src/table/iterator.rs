// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = SsTableIterator::get_iter_from_first(table.clone(), 0);

        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let blk_iter = SsTableIterator::get_iter_from_first(self.table.clone(), 0);

        self.blk_iter = blk_iter;
        self.blk_idx = 0;

        Ok(())
    }

    // /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (idx, blk_iter) = SsTableIterator::seek_to_key_inner(table.clone(), key)?;

        Ok(Self {
            blk_idx: idx,
            blk_iter,
            table,
        })
    }

    // /// Seek to the first key-value pair which >= `key`.
    // /// Note: You probably want to review the handout for detailed explanation when implementing
    // /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (idx, mut blk_iter) = SsTableIterator::seek_to_key_inner(self.table.clone(), key)?;

        std::mem::swap(&mut self.blk_iter, &mut blk_iter);
        self.blk_idx = idx;

        Ok(())
    }

    pub fn seek_to_key_inner(table: Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter = BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);

        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }

    pub(self) fn get_iter_from_first(table: Arc<SsTable>, idx: usize) -> BlockIterator {
        let block = table.read_block_cached(idx).unwrap();
        let blk_iter = BlockIterator::create_and_seek_to_first(block.clone());
        blk_iter
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter =
                    BlockIterator::create_and_seek_to_first(self.table.read_block_cached(self.blk_idx)?);
            }
        }
        Ok(())
    }
}
