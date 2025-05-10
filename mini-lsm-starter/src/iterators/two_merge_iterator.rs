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

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b };

        if key_cmp_eq(&iter.a, &iter.b) {
            iter.b.next()?;
        }

        Ok(iter)
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        let key: Self::KeyType<'_>;

        if key_cmp(&self.a, &self.b) {
            key = self.a.key();
        } else {
            key = self.b.key();
        }

        key
    }

    fn value(&self) -> &[u8] {
        let value: &[u8];

        if key_cmp(&self.a, &self.b) {
            value = self.a.value();
        } else {
            value = self.b.value();
        }

        value
    }

    fn is_valid(&self) -> bool {
        if key_cmp(&self.a, &self.b) {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if key_cmp(&self.a, &self.b) {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        if key_cmp_eq(&self.a, &self.b) {
            self.b.next()?;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}

/// Returns true only if the rhs is stricly less that the lhs
pub fn key_cmp<A, B>(a: &A, b: &B) -> bool
where
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
{
    if !a.is_valid() {
        return false;
    }
    if !b.is_valid() {
        return true;
    }

    match a.key().cmp(&b.key()) {
        std::cmp::Ordering::Less => true,

        _ => false,
    }
}

/// Returnsif the rhs is not equal to the lhs
pub fn key_cmp_eq<A, B>(a: &A, b: &B) -> bool
where
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
{
    if !a.is_valid() {
        return false;
    }
    if !b.is_valid() {
        return true;
    }
    match a.key().cmp(&b.key()) {
        std::cmp::Ordering::Equal => true,
        _ => false,
    }
}
