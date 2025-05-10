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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use nom::AsChar;

use super::{BlockMeta, SsTable, bloom::Bloom};
use crate::{
    block::{Block, BlockBuilder},
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Gets the first and last keys in a block and returns a pair of KeyBytes
///
pub fn get_keys(block: &Block) -> (KeyBytes, KeyBytes) {
    let mut first_entry = &block.data[block.offsets[0] as usize..];
    let mut last_entry = &block.data[block.offsets[block.offsets.len() - 1] as usize..];

    first_entry.get_u16() as usize;
    let key_len = first_entry.get_u16() as usize;
    let first_key = first_entry[..key_len].to_vec();

    let last_key_prefix = last_entry.get_u16() as usize;
    let last_key_len = last_entry.get_u16() as usize;

    // Appends only the overlap to the front of the last key
    let mut last_key = first_key[..last_key_prefix].to_vec();
    last_key.extend_from_slice(&last_entry[..key_len]);

    return (
        KeyBytes::from_bytes(Bytes::copy_from_slice(&first_key)),
        KeyBytes::from_bytes(Bytes::copy_from_slice(&last_key)),
    );
}

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    hashes: Vec<u32>,
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
            block_size,
            hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        (!self.builder.add(key, value)).then(|| {
            self.build_new();

            assert!(self.builder.add(key, value));

            self.first_key = key.raw_ref().to_vec();
            self.last_key = key.raw_ref().to_vec();

            return;
        });

        let key_hash = farmhash::fingerprint32(&key.raw_ref());
        self.hashes.push(key_hash);

        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_new();

        // Construct Bloom Filter
        let bloom = Some(Bloom::build_from_key_hashes(&self.hashes, 4)).expect("Bloom Filter");

        // Get first and last key
        let (first, last) = (
            &self.meta.first().unwrap().first_key,
            &self.meta.last().unwrap().last_key,
        );

        // Get meta data offset
        let meta_data_offset = self.data.len() as u32;

        // Encode meta data
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);

        buf.put_u32(meta_data_offset);

        let bloom_offset = buf.len() as u32;
        // Append bloom filter
        bloom.encode(&mut buf);

        buf.put_u32(bloom_offset);

        let bloom_filter_length = bloom.filter.len() + bloom.k.len();

        // Write encoded sst to file
        let file_obj = FileObject::create(path.as_ref(), buf)?;

        // Create sst object

        Ok(SsTable {
            id,
            file: file_obj,
            first_key: first.clone(),
            last_key: last.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_data_offset as usize,
            block_cache: Some(Arc::new(BlockCache::new((self.block_size * 3) as u64))),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    pub(self) fn build_new(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let meta_data = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        };

        let block = old_builder.build().encode();

        self.meta.push(meta_data);
        self.data.extend(block);
    }
}
