//! Transactions in WriteAhead Log.
use super::{AsKV, SyncID};
use crate::layers::bio::{BlockId, BlockSet, Buf, BufRef};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::os::Mutex;
use crate::prelude::*;
use crate::tx::Tx;

use core::cell::{RefCell, RefMut};
use core::fmt::Debug;
use core::mem::size_of;
use pod::Pod;

/// The bucket name of WAL.
pub(super) const BUCKET_WAL: &str = "WAL";

/// WAL append TX in `TxLsmTree`.
///
/// A `WalAppendTx` is used to append records, sync and discard WALs.
/// A WAL is storing, managing key-value records which are going to
/// put in `MemTable`. It's space is backed by a `TxLog` (L3).
#[derive(Clone)]
pub(super) struct WalAppendTx<D> {
    inner: Arc<Mutex<WalTxInner<D>>>,
}

struct WalTxInner<D> {
    /// Ongoing TX and appended WAL.
    wal_tx_and_log: Option<(RefCell<Tx>, Arc<TxLog<D>>)>,
    /// Current log ID of WAL for later discard.
    log_id: Option<TxLogId>,
    /// A buffer to cache appended records.
    record_buf: Vec<u8>,
    /// Store for WALs.
    tx_log_store: Arc<TxLogStore<D>>,
}

impl<D: BlockSet + 'static> WalAppendTx<D> {
    const BUF_CAP: usize = 1024 * BLOCK_SIZE;

    /// Prepare a new WAL TX.
    pub fn new(store: &Arc<TxLogStore<D>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WalTxInner {
                wal_tx_and_log: None,
                log_id: None,
                record_buf: Vec::with_capacity(Self::BUF_CAP),
                tx_log_store: store.clone(),
            })),
        }
    }

    /// Append phase for an Append TX, mainly to append newly records to the WAL.
    pub fn append<K: Pod, V: Pod>(&self, record: &dyn AsKV<K, V>) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.prepare()?;
        }

        {
            let record_buf = &mut inner.record_buf;
            record_buf.push(WalAppendFlag::Record as u8);
            record_buf.extend_from_slice(record.key().as_bytes());
            record_buf.extend_from_slice(record.value().as_bytes());
        }

        const MAX_RECORD_SIZE: usize = 49;
        if inner.record_buf.len() <= Self::BUF_CAP - MAX_RECORD_SIZE {
            return Ok(());
        }

        inner.align_record_buf();
        let (wal_tx, wal_log) = inner.wal_tx_and_log.as_ref().unwrap();
        self.flush_buf(&inner.record_buf, wal_tx.borrow_mut(), wal_log)?;
        inner.record_buf.clear();

        Ok(())
    }

    /// Commit phase for an Append TX, mainly to commit (or abort) the TX.
    pub fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() || inner.record_buf.is_empty() {
            return Ok(());
        }

        inner.align_record_buf();
        let (wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        self.flush_buf(&inner.record_buf, wal_tx.borrow_mut(), &wal_log)?;
        inner.record_buf.clear();

        drop(wal_log);
        let mut wal_tx = wal_tx.borrow_mut();
        wal_tx.commit()
    }

    /// Appends current sync ID to WAL.
    pub fn sync(&self, sync_id: SyncID) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.prepare()?;
        }
        inner.record_buf.push(WalAppendFlag::Sync as u8);
        inner.record_buf.extend_from_slice(&sync_id.to_le_bytes());

        inner.align_record_buf();
        let (wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        self.flush_buf(&inner.record_buf, wal_tx.borrow_mut(), &wal_log)?;
        inner.record_buf.clear();

        drop(wal_log);
        let mut wal_tx = wal_tx.borrow_mut();
        wal_tx.commit()
    }

    /// Flushes the buffer to the backed log.
    fn flush_buf(
        &self,
        record_buf: &[u8],
        mut wal_tx: RefMut<Tx>,
        log: &Arc<TxLog<D>>,
    ) -> Result<()> {
        debug_assert!(!record_buf.is_empty() && record_buf.len() % BLOCK_SIZE == 0);
        let res = wal_tx.context(|| {
            let buf = BufRef::try_from(record_buf).unwrap();
            log.append(buf)
        });
        if res.is_err() {
            wal_tx.abort();
        }
        res
    }

    /// Deletes the current WAL.
    pub fn discard(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        debug_assert!(inner.record_buf.is_empty());
        let _ = inner.wal_tx_and_log.take();
        let log_id = inner.log_id.take().unwrap();

        let store = inner.tx_log_store.clone();
        let mut wal_tx = store.new_tx();
        let res = wal_tx.context(move || store.delete_log(log_id));
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        wal_tx.commit()
    }

    /// Collect the synced records only in the WAL.
    pub fn collect_synced_records<K: Pod, V: Pod>(wal: &TxLog<D>) -> Result<Vec<(K, V)>> {
        let nblocks = wal.nblocks();
        let mut records = Vec::new();
        // TODO: Load the master sync ID from trusted storage

        let mut buf = Buf::alloc(nblocks)?;
        wal.read(0 as BlockId, buf.as_mut())?;
        let buf_slice = buf.as_slice();

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();
        let total_bytes = nblocks * BLOCK_SIZE;
        let mut offset = 0;
        let (mut max_sync_id, mut synced_len) = (None, 0);
        loop {
            const MIN_RECORD_SIZE: usize = 9;
            if offset > total_bytes - MIN_RECORD_SIZE {
                break;
            }

            let flag = WalAppendFlag::try_from(buf_slice[offset]);
            offset += 1;
            if flag.is_err() {
                continue;
            }

            match flag.unwrap() {
                WalAppendFlag::Record => {
                    let record = {
                        let k = K::from_bytes(&buf_slice[offset..offset + k_size]);
                        let v =
                            V::from_bytes(&buf_slice[offset + k_size..offset + k_size + v_size]);
                        offset += k_size + v_size;
                        (k, v)
                    };

                    records.push(record);
                }
                WalAppendFlag::Sync => {
                    let sync_id = SyncID::from_le_bytes(
                        buf_slice[offset..offset + size_of::<SyncID>()]
                            .try_into()
                            .unwrap(),
                    );
                    offset += size_of::<SyncID>();

                    let _ = max_sync_id.insert(sync_id);
                    synced_len = records.len();
                }
            }
        }

        if let Some(_max_sync_id) = max_sync_id {
            // TODO: Compare the read sync ID with the master sync ID
            records.truncate(synced_len);
            Ok(records)
        } else {
            Ok(vec![])
        }
    }
}

impl<D: BlockSet + 'static> WalTxInner<D> {
    /// Prepare phase for an Append TX, mainly to create new TX and WAL.
    pub fn prepare(&mut self) -> Result<()> {
        debug_assert!(self.wal_tx_and_log.is_none());
        let wal_tx_and_log = {
            let store = &self.tx_log_store;
            let mut wal_tx = store.new_tx();
            let log_id_opt = self.log_id.clone();
            let res = wal_tx.context(|| {
                if log_id_opt.is_some() {
                    store.open_log(log_id_opt.unwrap(), true)
                } else {
                    store.create_log(BUCKET_WAL)
                }
            });
            if res.is_err() {
                wal_tx.abort();
            }
            let wal_log = res?;
            let _ = self.log_id.insert(wal_log.id());
            (RefCell::new(wal_tx), wal_log)
        };

        let _ = self.wal_tx_and_log.insert(wal_tx_and_log);
        Ok(())
    }

    pub fn align_record_buf(&mut self) {
        let aligned_len = align_up(self.record_buf.len(), BLOCK_SIZE);
        self.record_buf.resize(aligned_len, 0);
    }
}

/// Two content kinds in a WAL.
#[derive(PartialEq, Eq, Debug)]
#[repr(u8)]
enum WalAppendFlag {
    Record = 13,
    Sync = 23,
}

impl TryFrom<u8> for WalAppendFlag {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            13 => Ok(WalAppendFlag::Record),
            23 => Ok(WalAppendFlag::Sync),
            _ => Err(Error::new(InvalidArgs)),
        }
    }
}
