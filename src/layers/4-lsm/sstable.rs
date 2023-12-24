//! Sorted String Table.
use super::mem_table::ValueEx;
use crate::layers::bio::{BlockSet, Buf, BufMut, BufRef, BID_SIZE};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::os::RwLock;
use crate::prelude::*;

use core::fmt::{self, Debug};
use core::hash::Hash;
use core::marker::PhantomData;
use core::mem::size_of;
use core::num::NonZeroUsize;
use core::ops::RangeInclusive;
use lru::LruCache;
use pod::Pod;

/// Sorted String Table (SST) for LSM-Tree
///
/// It's responsible for storing, managing key-value records on a `TxLog` (L3).
/// Records are serialized, sorted, organized on `TxLog`.
/// API: `build()`, `search()`, `range_search()`
pub(super) struct SSTable<K, V> {
    id: TxLogId,
    footer: Footer<K>,
    cache: RwLock<LruCache<BlockId, Arc<RecordBlock>>>,
    phantom: PhantomData<(K, V)>,
}

/// Footer of SSTable, contains metadata and index entry arrays of SSTable.
#[derive(Debug)]
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
#[derive(Debug)]
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

struct RecordBlock {
    buf: Vec<u8>,
}

struct RecordBlockIter<'a, K, V> {
    block: &'a RecordBlock,
    offset: usize,
    phantom: PhantomData<(K, V)>,
}

/// Format in `TxLog`:
/// ```text
/// |   [record]    |   [record]    |...|         Footer            |
/// |K|flag|V(V)|...|   [record]    |...| [IndexEntry] | FooterMeta |
/// |  BLOCK_SIZE   |  BLOCK_SIZE   |...|                           |
/// ```
// TODO: Continuously optimize `search()` & `build()`
// TODO: Support range query
impl<K: Ord + Pod + Hash + Debug, V: Pod + Debug> SSTable<K, V> {
    const K_SIZE: usize = size_of::<K>();
    const V_SIZE: usize = size_of::<V>();
    const MAX_RECORD_SIZE: usize = BID_SIZE + 1 + 2 * Self::V_SIZE;
    const INDEX_ENTRY_SIZE: usize = BID_SIZE + 2 * Self::K_SIZE;
    const CACHE_CAP: usize = 1024;

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

    pub fn is_within_range(&self, key: &K) -> bool {
        self.range().contains(key)
    }

    pub fn overlap_with(&self, rhs_range: &RangeInclusive<K>) -> bool {
        let lhs_range = self.range();
        !(lhs_range.end() < rhs_range.start() || lhs_range.start() > rhs_range.end())
    }

    pub fn search<D: BlockSet + 'static>(
        &self,
        key: &K,
        tx_log_store: &Arc<TxLogStore<D>>,
    ) -> Result<V> {
        debug_assert!(self.range().contains(key));
        let target_pos = self
            .footer
            .index
            .iter()
            .find_map(|entry| {
                if (entry.first..=entry.last).contains(key) {
                    Some(entry.pos)
                } else {
                    None
                }
            })
            .unwrap();

        // Search from cache first
        if let Some(target_value) = self.search_from_cache(key, target_pos) {
            return Ok(target_value);
        }

        // Search from log if cache misses
        let tx_log = tx_log_store.open_log(self.id, false)?;
        self.search_from_log(key, target_pos, &tx_log)
    }

    /// Search a target record in the SST (from cache).
    pub fn search_from_cache(&self, key: &K, pos: BlockId) -> Option<V> {
        let record_block = self.cache.write().get(&pos).cloned()?;
        let mut iter = RecordBlockIter::<'_, K, V>::from_block(&record_block);
        iter.find_map(|(k, v)| if k == *key { Some(v) } else { None })
    }

    /// Search a target record in the SST (from log).
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn search_from_log<D: BlockSet + 'static>(
        &self,
        key: &K,
        pos: BlockId,
        tx_log: &Arc<TxLog<D>>,
    ) -> Result<V> {
        debug_assert!(tx_log.id() == self.id());

        let mut record_block = RecordBlock::from_buf(vec![0; BLOCK_SIZE]);
        tx_log.read(pos, BufMut::try_from(&mut record_block.buf[..]).unwrap())?;

        let mut iter = RecordBlockIter::<'_, K, V>::from_block(&record_block);
        iter.find_map(|(k, v)| if k == *key { Some(v) } else { None })
            .ok_or(Error::with_msg(NotFound, "record not existed in the SST"))
    }

    /// Build a SST given a bunch of records, after build, the SST sealed.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn build<'a, D: BlockSet + 'static, I>(
        records_iter: I,
        sync_id: u64,
        tx_log: &'a Arc<TxLog<D>>,
    ) -> Result<Self>
    where
        I: Iterator<Item = (&'a K, &'a ValueEx<V>)>,
        Self: 'a,
    {
        let total_records = records_iter.size_hint().0;
        debug_assert!(total_records > 0);

        let mut cache = LruCache::new(NonZeroUsize::new(Self::CACHE_CAP).unwrap());
        let index_vec = Self::build_record_blocks(records_iter, total_records, tx_log, &mut cache)?;
        let footer = Self::build_footer::<D>(index_vec, total_records, sync_id, tx_log)?;

        Ok(Self {
            id: tx_log.id(),
            footer,
            cache: RwLock::new(cache),
            phantom: PhantomData,
        })
    }

    fn build_record_blocks<'a, D: BlockSet + 'static, I>(
        records_iter: I,
        total_records: usize,
        tx_log: &'a TxLog<D>,
        cache: &mut LruCache<BlockId, Arc<RecordBlock>>,
    ) -> Result<Vec<IndexEntry<K>>>
    where
        I: Iterator<Item = (&'a K, &'a ValueEx<V>)>,
        Self: 'a,
    {
        let mut index_vec =
            Vec::with_capacity(total_records / (BLOCK_SIZE / Self::MAX_RECORD_SIZE));
        let mut pos = 0 as BlockId;
        let mut first_k = None;
        let mut inner_offset = 0;

        let mut append_buf = Vec::with_capacity(BLOCK_SIZE);
        for (i, record) in records_iter.enumerate() {
            if inner_offset == 0 {
                debug_assert!(append_buf.is_empty());
                let _ = first_k.insert(*record.0);
            }

            append_buf.extend_from_slice(record.0.as_bytes());
            inner_offset += Self::K_SIZE;
            match record.1 {
                ValueEx::Synced(v) => {
                    append_buf.push(RecordFlag::Synced as u8);
                    append_buf.extend_from_slice(v.as_bytes());
                    inner_offset += 1 + Self::V_SIZE;
                }
                ValueEx::Unsynced(v) => {
                    append_buf.push(RecordFlag::Unsynced as u8);
                    append_buf.extend_from_slice(v.as_bytes());
                    inner_offset += 1 + Self::V_SIZE;
                }
                ValueEx::SyncedAndUnsynced(sv, usv) => {
                    append_buf.push(RecordFlag::SyncedAndUnsynced as u8);
                    append_buf.extend_from_slice(sv.as_bytes());
                    append_buf.extend_from_slice(usv.as_bytes());
                    inner_offset += Self::MAX_RECORD_SIZE;
                }
            }

            if BLOCK_SIZE - inner_offset >= Self::MAX_RECORD_SIZE && i != total_records - 1 {
                continue;
            }

            append_buf.resize(BLOCK_SIZE, 0);
            index_vec.push(IndexEntry {
                pos,
                first: first_k.unwrap(),
                last: *record.0,
            });

            let record_block = RecordBlock::from_buf(append_buf.clone());
            tx_log.append(BufRef::try_from(&record_block.buf[..]).unwrap())?;
            cache.put(pos, Arc::new(record_block));

            pos += 1;
            inner_offset = 0;
            append_buf.clear();
        }

        Ok(index_vec)
    }

    fn build_footer<'a, D: BlockSet + 'static>(
        index_vec: Vec<IndexEntry<K>>,
        total_records: usize,
        sync_id: u64,
        tx_log: &'a TxLog<D>,
    ) -> Result<Footer<K>>
    where
        Self: 'a,
    {
        let footer_buf_len = align_up(
            index_vec.len() * Self::INDEX_ENTRY_SIZE + FOOTER_META_SIZE,
            BLOCK_SIZE,
        );
        let mut append_buf = Vec::with_capacity(footer_buf_len);
        for entry in &index_vec {
            append_buf.extend_from_slice(&entry.pos.to_le_bytes());
            append_buf.extend_from_slice(entry.first.as_bytes());
            append_buf.extend_from_slice(entry.last.as_bytes());
        }
        append_buf.resize(footer_buf_len, 0);
        let meta = FooterMeta {
            index_nblocks: (footer_buf_len / BLOCK_SIZE) as _,
            num_index: index_vec.len() as _,
            total_records: total_records as _,
            sync_id,
        };
        append_buf[footer_buf_len - FOOTER_META_SIZE..].copy_from_slice(meta.as_bytes());
        tx_log.append(BufRef::try_from(&append_buf[..]).unwrap())?;

        Ok(Footer {
            meta,
            index: index_vec,
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
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(Self::CACHE_CAP).unwrap())),
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
        sync_id: u64,
        discard_unsynced: bool,
    ) -> Result<(Vec<(K, ValueEx<V>)>, Vec<(K, V)>)> {
        debug_assert!(sync_id >= self.sync_id());
        let all_synced = sync_id > self.sync_id();
        let mut records = Vec::with_capacity(self.footer.meta.total_records as _);
        let mut dropped_records = Vec::new();
        let mut rbuf = Buf::alloc(1)?;
        for entry in self.footer.index.iter() {
            let record_block_opt = self.cache.write().get(&entry.pos).cloned();
            let rbuf_slice = if let Some(record_block) = record_block_opt.as_ref() {
                &record_block.buf[..]
            } else {
                tx_log.read(entry.pos, rbuf.as_mut())?;
                rbuf.as_slice()
            };

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
                            if all_synced {
                                ValueEx::Synced(v)
                            } else if discard_unsynced {
                                dropped_records.push((k, v));
                                continue;
                            } else {
                                ValueEx::Unsynced(v)
                            }
                        }
                        RecordFlag::SyncedAndUnsynced => {
                            let sv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            let usv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            if all_synced {
                                dropped_records.push((k, sv));
                                ValueEx::Synced(usv)
                            } else if discard_unsynced {
                                dropped_records.push((k, usv));
                                ValueEx::Synced(sv)
                            } else {
                                ValueEx::SyncedAndUnsynced(sv, usv)
                            }
                        }
                        _ => unreachable!(),
                    }
                };

                records.push((k, v_ex));
            }
        }

        debug_assert!(records.is_sorted_by_key(|(k, _)| k));
        Ok((records, dropped_records))
    }
}

impl RecordBlock {
    pub fn from_buf(buf: Vec<u8>) -> Self {
        debug_assert_eq!(buf.len(), BLOCK_SIZE);
        Self { buf }
    }
}

impl<K: Ord + Pod + Hash + Debug, V: Pod + Debug> Iterator for RecordBlockIter<'_, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut offset = self.offset;
        let buf_slice = &self.block.buf;
        let (k_size, v_size) = (SSTable::<K, V>::K_SIZE, SSTable::<K, V>::V_SIZE);

        if offset + SSTable::<K, V>::MAX_RECORD_SIZE > BLOCK_SIZE {
            return None;
        }

        let key = K::from_bytes(&buf_slice[offset..offset + k_size]);
        offset += k_size;

        let flag = RecordFlag::from(buf_slice[offset]);
        offset += 1;
        if flag == RecordFlag::Invalid {
            return None;
        }

        let value = match flag {
            RecordFlag::Synced | RecordFlag::Unsynced => {
                let v = V::from_bytes(&buf_slice[offset..offset + v_size]);
                offset += v_size;
                v
            }
            RecordFlag::SyncedAndUnsynced => {
                let v = V::from_bytes(&buf_slice[offset + v_size..offset + 2 * v_size]);
                offset += 2 * v_size;
                v
            }
            _ => unreachable!(),
        };

        self.offset = offset;
        Some((key, value))
    }
}

impl<'a, K, V> RecordBlockIter<'a, K, V> {
    pub fn from_block(block: &'a RecordBlock) -> Self {
        Self {
            block,
            offset: 0,
            phantom: PhantomData,
        }
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
