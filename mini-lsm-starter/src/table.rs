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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

pub(self) fn check_in(block: Option<&&BlockMeta>, search_key: &Vec<u8>) -> bool {
    if block.is_none() {
        return false;
    }
    let meta_data = block.unwrap();

    let first_key = meta_data.first_key.raw_ref().to_vec();
    let last_key = meta_data.last_key.raw_ref().to_vec();

    if search_key >= &first_key && search_key <= &last_key {
        return true;
    }

    false
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        let mut estimated_size = 0;
        for meta in block_meta {
            // The size of offset
            estimated_size += std::mem::size_of::<u32>();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }

        buf.reserve(estimated_size);

        for blocks in block_meta {
            /*
               Encoding Schema
                _______________________________________________________________________________________
               |                                       BLOCK                                   |
               |_______________________________________________________________________________________|
               |   Offset       |   First-Key-Len    |   First-Key   |   Last-Key-Len  |   Last-Key    |
               |________________|____________________|_______________|_________________|_______________|
               |   4-bytes      |   2-bytes          |   n-bytes     |   2-bytes       |   n-bytes     |
               |________________|____________________|_______________|_________________|_______________|
            */

            // Offset
            buf.put_u32(blocks.offset as u32);
            // First-Key-Len
            let mut key_len = blocks.first_key.len();
            buf.put_u16(key_len as u16);
            // First-Key
            let mut key = blocks.first_key.as_key_slice().raw_ref();
            buf.put_slice(key);
            // Last-Key Len
            key_len = blocks.first_key.len();
            buf.put_u16(key_len as u16);
            // Last-Key
            key = blocks.last_key.as_key_slice().raw_ref();
            buf.put_slice(key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut mutable_buffer = buf;
        let mut block_meta: Vec<BlockMeta> = Vec::new();

        while mutable_buffer.has_remaining() {
            let offset = mutable_buffer.get_u32() as usize;

            let first_key_len = mutable_buffer.get_u16() as usize;
            let first_key = mutable_buffer.copy_to_bytes(first_key_len);

            let last_key_len = mutable_buffer.get_u16() as usize;
            let last_key = mutable_buffer.copy_to_bytes(last_key_len);

            block_meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }

        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_filter_offset_size = std::mem::size_of::<u32>() as u64;

        dbg!(bloom_filter_offset_size);
        let bloom_filter_offset = file
            .read(
                file.size() - bloom_filter_offset_size,
                bloom_filter_offset_size,
            )?
            .as_slice()
            .get_u32();

        let bloom_len = (file.size() - bloom_filter_offset_size) - bloom_filter_offset as u64;
        let bloom_filter_raw = file.read(bloom_filter_offset as u64, bloom_len)?;
        let bloom = Bloom::decode(&bloom_filter_raw)?;

        let data_len = std::mem::size_of::<u32>() as u64;

        let offset_index_bytes = file.read(bloom_filter_offset as u64 - data_len, data_len)?;
        let offset = (&offset_index_bytes[..]).get_u32() as u64;

        let capacity = (bloom_filter_offset as u64 - data_len - offset) as usize;
        let mut _meta: Vec<u8> = Vec::with_capacity(capacity);
        _meta = file.read(offset, capacity as u64)?;
        let meta_data_blocks = BlockMeta::decode_block_meta(&_meta[..]);

        Ok(Self {
            block_cache,
            first_key: meta_data_blocks.first().unwrap().first_key.clone(),
            last_key: meta_data_blocks.last().unwrap().last_key.clone(),
            block_meta: meta_data_blocks,
            block_meta_offset: offset as usize,
            bloom: Some(bloom),
            file,
            id,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta.get(block_idx).unwrap().offset;

        let next_block_offset = self
            .block_meta
            .get(block_idx + 1)
            .map(|x| x.offset)
            .unwrap_or(self.block_meta_offset);

        let dat_len = next_block_offset.saturating_sub(offset) as u64;

        let block_raw = self.file.read(offset as u64, dat_len)?;

        Ok(Arc::new(Block::decode(&block_raw[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let cache = self.block_cache.as_ref().unwrap().clone();

        let block = cache
            .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
            .unwrap();

        Ok(block)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let meta = self.block_meta.clone();
        let search_key = key.raw_ref().to_vec();

        let mut idx = meta
            .partition_point(|meta| meta.first_key.raw_ref() <= key.raw_ref())
            .saturating_sub(1); // <---- Use this more often

        if !check_in(meta.get(idx).as_ref(), &search_key) {
            if check_in(meta.get(idx + 1).as_ref(), &search_key) {
                idx += 1;
            }
        }

        idx
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
