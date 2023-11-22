//! Write Ahead Log.
use super::AsKv;
use crate::layers::bio::{BlockId, BlockSet, Buf};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::os::Mutex;
use crate::prelude::*;
use crate::tx::Tx;

use core::fmt::Debug;
use core::mem::size_of;
use pod::Pod;

pub(super) const BUCKET_WAL: &str = "WAL";

/// WAL append TX in `TxLsmTree`.
pub(super) struct WalAppendTx<D>
where
    D: BlockSet,
{
    inner: Mutex<WalAppendTxInner<D>>,
}

struct WalAppendTxInner<D> {
    wal_tx_and_log: Option<(Tx, Arc<TxLog<D>>)>, // Cache append TX and WAL log
    log_id: Option<TxLogId>,
    record_buf: Vec<u8>, // Cache appended WAL record
    tx_log_store: Arc<TxLogStore<D>>,
}

#[derive(PartialEq, Eq, Debug)]
enum WalAppendFlag {
    Record = 13,
    Sync = 23,
}

impl<D: BlockSet + 'static> WalAppendTx<D> {
    const BUF_CAP: usize = 128 * BLOCK_SIZE;

    pub fn new(store: &Arc<TxLogStore<D>>) -> Self {
        Self {
            inner: Mutex::new(WalAppendTxInner {
                wal_tx_and_log: None,
                log_id: None,
                record_buf: Vec::with_capacity(Self::BUF_CAP),
                tx_log_store: store.clone(),
            }),
        }
    }

    /// Append phase for Append TX, mainly to append newly records to WAL log.
    pub fn append<K: Pod, V: Pod>(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.perpare()?;
        }

        {
            let record_buf = &mut inner.record_buf;
            record_buf.push(WalAppendFlag::Record as u8);
            record_buf.extend_from_slice(record.key().as_bytes());
            record_buf.extend_from_slice(record.value().as_bytes());
        }

        if inner.record_buf.len() < Self::BUF_CAP {
            return Ok(());
        }

        let record_buf = inner.record_buf.clone();
        let (wal_tx, wal_log) = inner.wal_tx_and_log.as_mut().unwrap();
        self.flush_buf(&record_buf, wal_tx, wal_log)?;
        inner.record_buf.clear();

        Ok(())
    }

    /// Commit phase for Append TX, mainly to commit (or abort) the TX.
    pub fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            return Ok(());
        }

        let (mut wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        // Append cached records
        // Append master sync id
        self.flush_buf(&inner.record_buf, &mut wal_tx, &wal_log)?;
        inner.record_buf.clear();

        drop(wal_log);
        wal_tx.commit()
    }

    pub fn sync(&self, sync_id: u64) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.perpare()?;
        }
        inner.record_buf.push(WalAppendFlag::Sync as u8);
        inner.record_buf.extend_from_slice(&sync_id.to_le_bytes());

        let (mut wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        // Append cached records
        // Append master sync id
        self.flush_buf(&inner.record_buf, &mut wal_tx, &wal_log)?;
        inner.record_buf.clear();

        drop(wal_log);
        wal_tx.commit()
    }

    fn flush_buf(&self, record_buf: &[u8], tx: &mut Tx, log: &Arc<TxLog<D>>) -> Result<()> {
        let mut buf = Buf::alloc(align_up(record_buf.len(), BLOCK_SIZE) / BLOCK_SIZE)?;
        buf.as_mut_slice()[..record_buf.len()].copy_from_slice(&record_buf);
        let res = tx.context(|| {
            // Append cached records
            // Append master sync id
            log.append(buf.as_ref())
        });
        if res.is_err() {
            tx.abort();
        }
        res
    }

    pub fn discard(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        debug_assert!(inner.record_buf.is_empty());
        let store = inner.tx_log_store.clone();
        let _ = inner.wal_tx_and_log.take();
        let log_id = inner.log_id.take().unwrap();
        let mut wal_tx = store.new_tx();
        let res = wal_tx.context(move || store.delete_log(log_id));
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        wal_tx.commit()
    }

    pub fn collect_synced_records<K: Pod, V: Pod>(wal: &TxLog<D>) -> Result<Vec<(K, V)>> {
        let nblocks = wal.nblocks();
        let mut records = Vec::new();
        // TODO: Load master sync id from trusted storage

        let mut buf = Buf::alloc(nblocks)?;
        wal.read(0 as BlockId, buf.as_mut())?;
        let buf_slice = buf.as_slice();

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();
        let mut offset = 0;
        let (mut max_sync_id, mut synced_len) = (None, 0);
        loop {
            let flag = WalAppendFlag::try_from(buf_slice[offset]);
            if flag.is_err() || offset >= nblocks * BLOCK_SIZE - 1 {
                break;
            }
            offset += 1;
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
                    let sync_id =
                        u64::from_le_bytes(buf_slice[offset..offset + 8].try_into().unwrap());
                    let _ = max_sync_id.insert(sync_id);
                    synced_len = records.len();
                    offset += 8;
                }
            }
        }
        if let Some(_max_sync_id) = max_sync_id {
            // TODO: Compare read sync id with master sync id
            records.truncate(synced_len);
            Ok(records)
        } else {
            Ok(vec![])
        }
    }
}

impl<D: BlockSet + 'static> WalAppendTxInner<D> {
    /// Prepare phase for Append TX, mainly to create new TX and WAL log.
    fn perpare(&mut self) -> Result<()> {
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
            (wal_tx, wal_log)
        };
        let _ = self.wal_tx_and_log.insert(wal_tx_and_log);
        Ok(())
    }
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
