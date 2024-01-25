//! Sorted String Table.
use super::mem_table::ValueEx;
use super::tx_lsm_tree::AsKVex;
use super::{RangeQueryCtx, RecordKey, RecordValue, SyncID};
use crate::layers::bio::{BlockSet, Buf, BufMut, BufRef, BID_SIZE};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::os::RwLock;
use crate::prelude::*;

use core::fmt::{self, Debug};
use core::marker::PhantomData;
use core::mem::size_of;
use core::num::NonZeroUsize;
use core::ops::RangeInclusive;
use lru::LruCache;
use pod::Pod;

/// Sorted String Table (SST) for `TxLsmTree`.
///
/// Responsible for storing, managing key-value records on a `TxLog` (L3).
/// Records are serialized, sorted, organized on the `TxLog`.
/// Supports three access modes: point query, range query and whole scan.
pub(super) struct SSTable<K, V> {
    id: TxLogId,
    footer: Footer<K>,
    cache: RwLock<LruCache<BlockId, Arc<RecordBlock>>>,
    phantom: PhantomData<(K, V)>,
}

/// Footer of a `SSTable`, contains metadata of itself
/// index entries for locating record blocks.
#[derive(Debug)]
struct Footer<K> {
    meta: FooterMeta,
    index: Vec<IndexEntry<K>>,
}

/// Footer metadata to describe a `SSTable`.
#[repr(C)]
#[derive(Copy, Clone, Pod, Debug)]
struct FooterMeta {
    index_nblocks: u16,
    num_index: u16,
    total_records: u32,
    sync_id: SyncID,
}
const FOOTER_META_SIZE: usize = size_of::<FooterMeta>();

/// Index entry to describe a `RecordBlock` in a `SSTable`.
#[derive(Debug)]
struct IndexEntry<K> {
    pos: BlockId,
    first: K,
    last: K,
}

/// A block full of serialized records.
// TODO: Choose an appropriate record block size.
struct RecordBlock {
    buf: Vec<u8>,
}

/// Accessor for a query.
enum QueryAccessor<K> {
    Point(K),
    Range(RangeInclusive<K>),
}

/// Iterator over `RecordBlock` for query purpose.
struct BlockQueryIter<'a, K, V> {
    block: &'a RecordBlock,
    offset: usize,
    accessor: &'a QueryAccessor<K>,
    phantom: PhantomData<(K, V)>,
}

/// Accessor for a whole scan
struct ScanAccessor<K, V> {
    all_synced: bool,
    discard_unsynced: bool,
    dropped_records: Vec<(K, V)>,
}

/// Iterator over `RecordBlock` for scan purpose.
struct BlockScanIter<'a, K, V> {
    block: &'a RecordBlock,
    offset: usize,
    accessor: &'a mut ScanAccessor<K, V>,
}

/// Format on a `TxLog`:
///
/// ```text
/// |   [record]    |   [record]    |...|         Footer            |
/// |K|flag|V(V)|...|   [record]    |...| [IndexEntry] | FooterMeta |
/// |  BLOCK_SIZE   |  BLOCK_SIZE   |...|                           |
/// ```
impl<K: RecordKey<K>, V: RecordValue> SSTable<K, V> {
    const K_SIZE: usize = size_of::<K>();
    const V_SIZE: usize = size_of::<V>();
    const MAX_RECORD_SIZE: usize = BID_SIZE + 1 + 2 * Self::V_SIZE;
    const INDEX_ENTRY_SIZE: usize = BID_SIZE + 2 * Self::K_SIZE;
    const CACHE_CAP: usize = 1024;

    /// Return the ID of this `SSTable`, which is the same ID
    /// to the underlying `TxLog`.
    pub fn id(&self) -> TxLogId {
        self.id
    }

    /// Return the sync ID of this `SSTable`, it may be smaller than the
    /// current master sync ID.
    pub fn sync_id(&self) -> SyncID {
        self.footer.meta.sync_id
    }

    /// The range of keys covered by this `SSTable`.
    pub fn range(&self) -> RangeInclusive<K> {
        RangeInclusive::new(
            self.footer.index[0].first,
            self.footer.index[self.footer.meta.num_index as usize - 1].last,
        )
    }

    /// Whether the target key is within the range, "within the range" doesn't mean
    /// the `SSTable` do have this key.
    pub fn is_within_range(&self, key: &K) -> bool {
        self.range().contains(key)
    }

    /// Whether the target range is overlapped with the range of this `SSTable`.
    pub fn overlap_with(&self, rhs_range: &RangeInclusive<K>) -> bool {
        let lhs_range = self.range();
        !(lhs_range.end() < rhs_range.start() || lhs_range.start() > rhs_range.end())
    }

    /// Accessing functions below

    /// Point query.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn access_point<D: BlockSet + 'static>(
        &self,
        key: &K,
        tx_log_store: &Arc<TxLogStore<D>>,
    ) -> Result<V> {
        debug_assert!(self.range().contains(key));
        let target_rb_pos = self
            .footer
            .index
            .iter()
            .find_map(|entry| {
                if entry.is_within_range(key) {
                    Some(entry.pos)
                } else {
                    None
                }
            })
            .unwrap();

        let accessor = QueryAccessor::Point(*key);
        let target_rb = self.target_record_block(target_rb_pos, tx_log_store)?;

        let mut iter = BlockQueryIter::<'_, K, V> {
            block: &target_rb,
            offset: 0,
            accessor: &accessor,
            phantom: PhantomData,
        };

        iter.find_map(|(k, v_opt)| if k == *key { v_opt } else { None })
            .ok_or(Error::with_msg(NotFound, "target value not found in SST"))
    }

    /// Range query.    
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn access_range<D: BlockSet + 'static>(
        &self,
        range_query_ctx: &mut RangeQueryCtx<K, V>,
        tx_log_store: &Arc<TxLogStore<D>>,
    ) -> Result<()> {
        debug_assert!(!range_query_ctx.is_completed());
        let range_uncompleted = range_query_ctx.range_uncompleted().unwrap();
        let target_rbs = self.footer.index.iter().filter_map(|entry| {
            if entry.overlap_with(&range_uncompleted) {
                Some(entry.pos)
            } else {
                None
            }
        });

        let accessor = QueryAccessor::Range(range_uncompleted.clone());
        for target_rb_pos in target_rbs {
            let target_rb = self.target_record_block(target_rb_pos, tx_log_store)?;

            let iter = BlockQueryIter::<'_, K, V> {
                block: &target_rb,
                offset: 0,
                accessor: &accessor,
                phantom: PhantomData,
            };

            let targets: Vec<_> = iter
                .filter_map(|(k, v_opt)| {
                    if range_uncompleted.contains(&k) {
                        Some((k, v_opt.unwrap()))
                    } else {
                        None
                    }
                })
                .collect();
            for (target_k, target_v) in targets {
                range_query_ctx.complete(target_k, target_v);
            }
        }
        Ok(())
    }

    /// Locate the target record block given its position, it
    /// resides in either the cache or the log.
    fn target_record_block<D: BlockSet + 'static>(
        &self,
        target_pos: BlockId,
        tx_log_store: &Arc<TxLogStore<D>>,
    ) -> Result<Arc<RecordBlock>> {
        let cached_rb_opt = self.cache.write().get(&target_pos).cloned();
        let target_rb = if let Some(cached_rb) = cached_rb_opt {
            cached_rb
        } else {
            let mut rb = RecordBlock::from_buf(vec![0; BLOCK_SIZE]);
            let tx_log = tx_log_store.open_log(self.id, false)?;
            tx_log.read(target_pos, BufMut::try_from(&mut rb.buf[..]).unwrap())?;
            Arc::new(rb)
        };
        Ok(target_rb)
    }

    /// Scan the whole SST and collect all records.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn access_scan<D: BlockSet + 'static>(
        &self,
        tx_log: &Arc<TxLog<D>>,
        sync_id: SyncID,
        discard_unsynced: bool,
    ) -> Result<(Vec<(K, ValueEx<V>)>, Vec<(K, V)>)> {
        debug_assert!(sync_id >= self.sync_id());
        let all_synced = sync_id > self.sync_id();
        let mut records = Vec::with_capacity(self.footer.meta.total_records as _);

        let mut accessor = ScanAccessor {
            all_synced,
            discard_unsynced,
            dropped_records: Vec::new(),
        };
        let mut curr_record_block = RecordBlock::from_buf(vec![0u8; BLOCK_SIZE]);

        for entry in self.footer.index.iter() {
            let record_block_opt = self.cache.write().get(&entry.pos).cloned();
            let record_block = if let Some(record_block) = record_block_opt.as_ref() {
                record_block
            } else {
                tx_log.read(
                    entry.pos,
                    BufMut::try_from(&mut curr_record_block.buf[..]).unwrap(),
                )?;
                &curr_record_block
            };

            let iter = BlockScanIter {
                block: record_block,
                offset: 0,
                accessor: &mut accessor,
            };
            records.extend(iter);
        }

        debug_assert!(records.is_sorted_by_key(|(k, _)| k));
        Ok((records, accessor.dropped_records))
    }

    /// Building functions below

    /// Builds a SST given a bunch of records, after the SST becomes immutable.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn build<'a, D: BlockSet + 'static, I, KVex>(
        records_iter: I,
        sync_id: SyncID,
        tx_log: &'a Arc<TxLog<D>>,
    ) -> Result<Self>
    where
        I: Iterator<Item = KVex>,
        KVex: AsKVex<K, V>,
        Self: 'a,
    {
        let mut cache = LruCache::new(NonZeroUsize::new(Self::CACHE_CAP).unwrap());
        let (total_records, index_vec) =
            Self::build_record_blocks(records_iter, tx_log, &mut cache)?;
        let footer = Self::build_footer::<D>(index_vec, total_records, sync_id, tx_log)?;

        Ok(Self {
            id: tx_log.id(),
            footer,
            cache: RwLock::new(cache),
            phantom: PhantomData,
        })
    }

    /// Builds all the record blocks from the given records. Put the blocks to the log
    /// and the cache.
    fn build_record_blocks<'a, D: BlockSet + 'static, I, KVex>(
        records_iter: I,
        tx_log: &'a TxLog<D>,
        cache: &mut LruCache<BlockId, Arc<RecordBlock>>,
    ) -> Result<(usize, Vec<IndexEntry<K>>)>
    where
        I: Iterator<Item = KVex>,
        KVex: AsKVex<K, V>,
        Self: 'a,
    {
        let mut index_vec = Vec::new();
        let mut total_records = 0;
        let mut pos = 0 as BlockId;
        let (mut first_k, mut curr_k) = (None, None);
        let mut inner_offset = 0;

        let mut block_buf = Vec::with_capacity(BLOCK_SIZE);
        for (nth, kv_ex) in records_iter.enumerate() {
            let (key, value_ex) = (*kv_ex.key(), kv_ex.value_ex());
            total_records += 1;

            if inner_offset == 0 {
                debug_assert!(block_buf.is_empty());
                let _ = first_k.insert(key);
            }
            let _ = curr_k.insert(key);

            block_buf.extend_from_slice(key.as_bytes());
            inner_offset += Self::K_SIZE;

            match value_ex {
                ValueEx::Synced(v) => {
                    block_buf.push(RecordFlag::Synced as u8);
                    block_buf.extend_from_slice(v.as_bytes());
                    inner_offset += 1 + Self::V_SIZE;
                }
                ValueEx::Unsynced(v) => {
                    block_buf.push(RecordFlag::Unsynced as u8);
                    block_buf.extend_from_slice(v.as_bytes());
                    inner_offset += 1 + Self::V_SIZE;
                }
                ValueEx::SyncedAndUnsynced(sv, usv) => {
                    block_buf.push(RecordFlag::SyncedAndUnsynced as u8);
                    block_buf.extend_from_slice(sv.as_bytes());
                    block_buf.extend_from_slice(usv.as_bytes());
                    inner_offset += Self::MAX_RECORD_SIZE;
                }
            }

            let cap_remained = BLOCK_SIZE - inner_offset;
            if cap_remained >= Self::MAX_RECORD_SIZE {
                continue;
            }

            let index_entry = IndexEntry {
                pos,
                first: first_k.unwrap(),
                last: key,
            };
            build_one_record_block(&index_entry, &mut block_buf, tx_log, cache)?;
            index_vec.push(index_entry);

            pos += 1;
            inner_offset = 0;
            block_buf.clear();
        }
        debug_assert!(total_records > 0);

        if !block_buf.is_empty() {
            let last_entry = IndexEntry {
                pos,
                first: first_k.unwrap(),
                last: curr_k.unwrap(),
            };
            build_one_record_block(&last_entry, &mut block_buf, tx_log, cache)?;
            index_vec.push(last_entry);
        }

        fn build_one_record_block<K: RecordKey<K>, D: BlockSet + 'static>(
            entry: &IndexEntry<K>,
            buf: &mut Vec<u8>,
            tx_log: &TxLog<D>,
            cache: &mut LruCache<BlockId, Arc<RecordBlock>>,
        ) -> Result<()> {
            buf.resize(BLOCK_SIZE, 0);
            let record_block = RecordBlock::from_buf(buf.clone());

            tx_log.append(BufRef::try_from(record_block.as_slice()).unwrap())?;
            cache.put(entry.pos, Arc::new(record_block));
            Ok(())
        }

        Ok((total_records, index_vec))
    }

    /// Builds the footer from the given index entries. The footer block will be appended
    /// to the SST log's end.
    fn build_footer<'a, D: BlockSet + 'static>(
        index_vec: Vec<IndexEntry<K>>,
        total_records: usize,
        sync_id: SyncID,
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

    /// Builds a SST from a `TxLog`, loads the footer and the index blocks.
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
}

impl<K: RecordKey<K>> IndexEntry<K> {
    pub fn range(&self) -> RangeInclusive<K> {
        self.first..=self.last
    }

    pub fn is_within_range(&self, key: &K) -> bool {
        self.range().contains(key)
    }

    pub fn overlap_with(&self, rhs_range: &RangeInclusive<K>) -> bool {
        let lhs_range = self.range();
        !(lhs_range.end() < rhs_range.start() || lhs_range.start() > rhs_range.end())
    }
}

impl RecordBlock {
    pub fn from_buf(buf: Vec<u8>) -> Self {
        debug_assert_eq!(buf.len(), BLOCK_SIZE);
        Self { buf }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }
}

impl<K: RecordKey<K>> QueryAccessor<K> {
    pub fn hit_target(&self, target: &K) -> bool {
        match self {
            QueryAccessor::Point(k) => k == target,
            QueryAccessor::Range(range) => range.contains(target),
        }
    }
}

impl<K: RecordKey<K>, V: RecordValue> Iterator for BlockQueryIter<'_, K, V> {
    type Item = (K, Option<V>);

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

        let hit_target = self.accessor.hit_target(&key);
        let value_opt = match flag {
            RecordFlag::Synced | RecordFlag::Unsynced => {
                let v_opt = if hit_target {
                    Some(V::from_bytes(&buf_slice[offset..offset + v_size]))
                } else {
                    None
                };
                offset += v_size;
                v_opt
            }
            RecordFlag::SyncedAndUnsynced => {
                let v_opt = if hit_target {
                    Some(V::from_bytes(
                        &buf_slice[offset + v_size..offset + 2 * v_size],
                    ))
                } else {
                    None
                };
                offset += 2 * v_size;
                v_opt
            }
            _ => unreachable!(),
        };

        self.offset = offset;
        Some((key, value_opt))
    }
}

impl<K: RecordKey<K>, V: RecordValue> Iterator for BlockScanIter<'_, K, V> {
    type Item = (K, ValueEx<V>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut offset = self.offset;
        let buf_slice = &self.block.buf;
        let (k_size, v_size) = (SSTable::<K, V>::K_SIZE, SSTable::<K, V>::V_SIZE);
        let (all_synced, discard_unsynced, dropped_records) = (
            self.accessor.all_synced,
            self.accessor.discard_unsynced,
            &mut self.accessor.dropped_records,
        );

        let (key, value_ex) = loop {
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

            let v_ex = match flag {
                RecordFlag::Synced => {
                    let v = V::from_bytes(&buf_slice[offset..offset + v_size]);
                    offset += v_size;
                    ValueEx::Synced(v)
                }
                RecordFlag::Unsynced => {
                    let v = V::from_bytes(&buf_slice[offset..offset + v_size]);
                    offset += v_size;
                    if all_synced {
                        ValueEx::Synced(v)
                    } else if discard_unsynced {
                        dropped_records.push((key, v));
                        continue;
                    } else {
                        ValueEx::Unsynced(v)
                    }
                }
                RecordFlag::SyncedAndUnsynced => {
                    let sv = V::from_bytes(&buf_slice[offset..offset + v_size]);
                    offset += v_size;
                    let usv = V::from_bytes(&buf_slice[offset..offset + v_size]);
                    offset += v_size;
                    if all_synced {
                        dropped_records.push((key, sv));
                        ValueEx::Synced(usv)
                    } else if discard_unsynced {
                        dropped_records.push((key, usv));
                        ValueEx::Synced(sv)
                    } else {
                        ValueEx::SyncedAndUnsynced(sv, usv)
                    }
                }
                _ => unreachable!(),
            };
            break (key, v_ex);
        };

        self.offset = offset;
        Some((key, value_ex))
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

/// Flag bit for records in SSTable.
#[derive(PartialEq, Eq, Debug)]
enum RecordFlag {
    Synced = 7,
    Unsynced = 11,
    SyncedAndUnsynced = 19,
    Invalid = 0,
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
