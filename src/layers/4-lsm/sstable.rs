//! Sorted String Table.
use super::mem_table::ValueEx;
use crate::layers::bio::{BlockSet, Buf, BID_SIZE};
use crate::layers::log::{TxLog, TxLogId};
use crate::prelude::*;

use core::fmt::{self, Debug};
use core::marker::PhantomData;
use core::mem::size_of;
use core::ops::RangeInclusive;
use pod::Pod;

/// Sorted String Table (SST) for LSM-Tree
///
/// format:
/// ```text
/// |   [record]    |   [record]    |...|         Footer            |
/// |K|flag|V(V)|...|   [record]    |...| [IndexEntry] | FooterMeta |
/// |  BLOCK_SIZE   |  BLOCK_SIZE   |...|                           |
/// ```
///
// TODO: Add bloom filter and second-level index
pub(super) struct SSTable<K, V> {
    // Cache txlog id, and footer block
    id: TxLogId,
    footer: Footer<K>,
    phantom: PhantomData<(K, V)>,
}

/// Footer of SSTable, contains metadata and index entry arrays of SSTable.
struct Footer<K> {
    meta: FooterMeta,
    index: Vec<IndexEntry<K>>,
}

/// Footer metadata to describe a SSTable.
#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
struct FooterMeta {
    index_nblocks: u16,
    num_index: u16,
    total_records: u32,
    sync_id: u64,
}
const FOOTER_META_SIZE: usize = size_of::<FooterMeta>();

/// Index entry of SSTable.
struct IndexEntry<K> {
    pos: BlockId,
    first: K,
    last: K,
}

/// Flag bit for record in SSTable.
#[derive(PartialEq, Eq, Debug)]
enum RecordFlag {
    Invalid = 0,
    Synced = 7,
    Unsynced = 11,
    SyncedAndUnsynced = 19,
}

impl<K: Ord + Pod + Debug, V: Pod> SSTable<K, V> {
    const K_SIZE: usize = size_of::<K>();
    const V_SIZE: usize = size_of::<V>();
    const MAX_RECORD_SIZE: usize = BID_SIZE + 1 + 2 * Self::V_SIZE;
    const INDEX_ENTRY_SIZE: usize = BID_SIZE + Self::K_SIZE;
    // TODO: Optimize search&build

    pub fn id(&self) -> TxLogId {
        self.id
    }

    pub fn sync_id(&self) -> u64 {
        self.footer.meta.sync_id
    }

    pub fn range(&self) -> RangeInclusive<K> {
        RangeInclusive::new(
            self.footer.index[0].first,
            self.footer.index[self.footer.meta.num_index as usize - 1].last,
        )
    }

    pub fn overlap_with(&self, rhs_range: &RangeInclusive<K>) -> bool {
        let lhs_range = self.range();
        !(lhs_range.end() < rhs_range.start() || lhs_range.start() > rhs_range.end())
    }

    pub fn search<D: BlockSet + 'static>(&self, key: &K, tx_log: &Arc<TxLog<D>>) -> Option<V> {
        let target_block_pos = self.search_in_cache(key)?;
        self.search_in_log(key, target_block_pos, tx_log).ok()
    }

    /// Search a target records block position in the SST (from cache).
    fn search_in_cache(&self, key: &K) -> Option<BlockId> {
        // TODO: Use iter::find
        for (i, entry) in self.footer.index.iter().enumerate() {
            if (entry.first..=entry.last).contains(key) {
                return Some(self.footer.index[i].pos);
            }
        }
        None
    }

    /// Search a target record in the SST (from log).
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn search_in_log<D: BlockSet + 'static>(
        &self,
        key: &K,
        target_pos: BlockId,
        tx_log: &Arc<TxLog<D>>,
    ) -> Result<V> {
        debug_assert!(tx_log.id() == self.id());
        let mut rbuf = Buf::alloc(1)?;
        tx_log.read(target_pos, rbuf.as_mut())?;
        // Search in the records block
        let rbuf_slice = rbuf.as_slice();
        let mut offset = 0;
        loop {
            if offset + Self::MAX_RECORD_SIZE > BLOCK_SIZE {
                break;
            }
            let k = K::from_bytes(&rbuf_slice[offset..offset + Self::K_SIZE]);
            offset += Self::K_SIZE;
            let flag = RecordFlag::from(rbuf_slice[offset]);
            offset += 1;
            if flag == RecordFlag::Invalid {
                break;
            }
            let target_value = match flag {
                RecordFlag::Synced | RecordFlag::Unsynced => {
                    let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                    offset += Self::V_SIZE;
                    v
                }
                RecordFlag::SyncedAndUnsynced => {
                    let v = V::from_bytes(
                        &rbuf_slice[offset + Self::V_SIZE..offset + 2 * Self::V_SIZE],
                    );
                    offset += 2 * Self::V_SIZE;
                    v
                }
                _ => unreachable!(),
            };
            if k != *key {
                continue;
            }
            return Ok(target_value);
        }

        return_errno_with_msg!(NotFound, "record not existed in the tx log");
    }

    /// Build a SST given a bunch of records, after build, the SST sealed.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn build<D: BlockSet + 'static>(
        records: &[(K, ValueEx<V>)],
        sync_id: u64,
        tx_log: &TxLog<D>,
    ) -> Result<Self> {
        debug_assert!(!records.is_empty());
        let total_records = records.len();
        let mut index = Vec::new();
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        let mut append_buf = Buf::alloc(1)?;
        let mut pos = 0 as BlockId;
        let mut first_k = None;
        for (i, record) in records.iter().enumerate() {
            if buf.is_empty() {
                let _ = first_k.insert(record.0);
            }
            buf.extend_from_slice(record.0.as_bytes());
            match record.1 {
                ValueEx::Synced(v) => {
                    buf.push(RecordFlag::Synced as u8);
                    buf.extend_from_slice(v.as_bytes());
                }
                ValueEx::Unsynced(v) => {
                    buf.push(RecordFlag::Unsynced as u8);
                    buf.extend_from_slice(v.as_bytes());
                }
                ValueEx::SyncedAndUnsynced(cv, ucv) => {
                    buf.push(RecordFlag::SyncedAndUnsynced as u8);
                    buf.extend_from_slice(cv.as_bytes());
                    buf.extend_from_slice(ucv.as_bytes());
                }
            }
            if BLOCK_SIZE - buf.len() < Self::MAX_RECORD_SIZE || i == total_records - 1 {
                append_buf.as_mut_slice()[..buf.len()].copy_from_slice(&buf);
                tx_log.append(append_buf.as_ref())?;
                index.push(IndexEntry {
                    pos,
                    first: first_k.unwrap(),
                    last: record.0,
                });
                pos += 1;
                buf.clear();
                append_buf.as_mut_slice().fill(0);
            }
        }

        debug_assert!(buf.is_empty());
        for entry in &index {
            buf.extend_from_slice(&entry.pos.to_le_bytes());
            buf.extend_from_slice(entry.first.as_bytes());
        }
        let index_nblocks = {
            let nblocks = align_up(buf.len(), BLOCK_SIZE) / BLOCK_SIZE;
            if nblocks * BLOCK_SIZE - buf.len() <= FOOTER_META_SIZE {
                nblocks
            } else {
                nblocks + 1
            }
        };
        let meta = FooterMeta {
            index_nblocks: index_nblocks as _,
            num_index: index.len() as _,
            total_records: total_records as _,
            sync_id,
        };
        let mut append_buf = Buf::alloc(index_nblocks)?;
        append_buf.as_mut_slice()[..buf.len()].copy_from_slice(&buf);
        append_buf.as_mut_slice()[index_nblocks * BLOCK_SIZE - FOOTER_META_SIZE..]
            .copy_from_slice(meta.as_bytes());
        tx_log.append(append_buf.as_ref())?;

        Ok(Self {
            id: tx_log.id(),
            footer: Footer { meta, index },
            phantom: PhantomData,
        })
    }

    /// Build a SST from a tx log, load index block to cache.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn from_log<D: BlockSet + 'static>(tx_log: &Arc<TxLog<D>>) -> Result<Self> {
        let nblocks = tx_log.nblocks();
        let mut rbuf = Buf::alloc(1)?;
        // Load footer block (last block)
        tx_log.read(nblocks - 1, rbuf.as_mut())?;

        let meta = FooterMeta::from_bytes(&rbuf.as_slice()[BLOCK_SIZE - FOOTER_META_SIZE..]);
        let mut rbuf = Buf::alloc(meta.index_nblocks as _)?;
        tx_log.read(nblocks - meta.index_nblocks as usize, rbuf.as_mut())?;
        let mut index = Vec::with_capacity(meta.num_index as _);
        for i in 0..meta.num_index as _ {
            let buf =
                &rbuf.as_slice()[i * Self::INDEX_ENTRY_SIZE..(i + 1) * Self::INDEX_ENTRY_SIZE];
            let pos = BlockId::from_le_bytes(buf[..BID_SIZE].try_into().unwrap());
            let first = K::from_bytes(&buf[BID_SIZE..BID_SIZE + Self::K_SIZE]);
            let last =
                K::from_bytes(&buf[Self::INDEX_ENTRY_SIZE - Self::K_SIZE..Self::INDEX_ENTRY_SIZE]);
            index.push(IndexEntry { pos, first, last })
        }

        let footer = Footer { meta, index };
        Ok(Self {
            id: tx_log.id(),
            footer,
            phantom: PhantomData,
        })
    }

    /// Collect all records from a SST.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn collect_all_records<D: BlockSet + 'static>(
        &self,
        tx_log: &Arc<TxLog<D>>,
    ) -> Result<Vec<(K, ValueEx<V>)>> {
        let mut records = Vec::with_capacity(self.footer.meta.total_records as _);
        let mut rbuf = Buf::alloc(1)?;
        for entry in self.footer.index.iter() {
            tx_log.read(entry.pos, rbuf.as_mut())?;
            let rbuf_slice = rbuf.as_slice();

            let mut offset = 0;
            loop {
                if offset + Self::MAX_RECORD_SIZE > BLOCK_SIZE {
                    break;
                }

                let k = K::from_bytes(&rbuf_slice[offset..offset + Self::K_SIZE]);
                offset += Self::K_SIZE;
                let v_ex = {
                    let flag = RecordFlag::from(rbuf_slice[offset]);
                    offset += 1;
                    if flag == RecordFlag::Invalid {
                        break;
                    }
                    match flag {
                        RecordFlag::Synced => {
                            let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::Synced(v)
                        }
                        RecordFlag::Unsynced => {
                            let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::Unsynced(v)
                        }
                        RecordFlag::SyncedAndUnsynced => {
                            let cv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            let ucv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::SyncedAndUnsynced(cv, ucv)
                        }
                        _ => unreachable!(),
                    }
                };
                records.push((k, v_ex));
            }
        }

        Ok(records)
    }
}

impl From<u8> for RecordFlag {
    fn from(value: u8) -> Self {
        match value {
            7 => RecordFlag::Synced,
            11 => RecordFlag::Unsynced,
            19 => RecordFlag::SyncedAndUnsynced,
            _ => RecordFlag::Invalid,
        }
    }
}

impl<K: Debug, V> Debug for SSTable<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SSTable")
            .field("footer", &self.footer.meta)
            .field(
                "range",
                &RangeInclusive::new(
                    &self.footer.index[0].first,
                    &self.footer.index[self.footer.meta.num_index as usize - 1].last,
                ),
            )
            .finish()
    }
}