use std::time::{Duration, Instant};

pub static mut COST: Cost = Cost::new(true);

#[derive(Debug)]
pub struct Cost {
    pub enable: bool,
    pub data: DataCost,
    pub lsm: LsmCost,
    pub amount: Amount,
}

#[derive(Debug)]
pub struct DataCost {
    pub alloc: Duration,
    pub enc: Duration,
    pub dec: Duration,
    pub read: Duration,
    pub write: Duration,
    pub sync: Duration,
    pub total: Duration,
}

#[derive(Debug)]
pub struct LsmCost {
    pub get: Duration,
    pub put: Duration,
    pub put_wal: Duration,
    pub put_compaction_minor: Duration,
    pub put_compaction_major: Duration,
    pub sync: Duration,
    pub sync_wal: Duration,
    pub sync_memtable: Duration,
    pub total: Duration,
}

#[derive(Debug, Default)]
pub struct Amount {
    pub data: usize,
    pub lsm: usize,
    pub journal: usize,
    pub total: usize,
}

impl Cost {
    pub const fn new(enable: bool) -> Self {
        Self {
            enable,
            data: DataCost {
                alloc: Duration::ZERO,
                enc: Duration::ZERO,
                dec: Duration::ZERO,
                read: Duration::ZERO,
                write: Duration::ZERO,
                sync: Duration::ZERO,
                total: Duration::ZERO,
            },
            lsm: LsmCost {
                get: Duration::ZERO,
                put: Duration::ZERO,
                put_wal: Duration::ZERO,
                put_compaction_minor: Duration::ZERO,
                put_compaction_major: Duration::ZERO,
                sync: Duration::ZERO,
                sync_wal: Duration::ZERO,
                sync_memtable: Duration::ZERO,
                total: Duration::ZERO,
            },
            amount: Amount {
                data: 0,
                lsm: 0,
                journal: 0,
                total: 0,
            },
        }
    }

    pub fn activate() -> Instant {
        Instant::now()
    }

    pub fn acc_data_write(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.data.write += elapsed;
            COST.data.total += elapsed;
        }
    }

    pub fn acc_data_sync(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.data.sync += elapsed;
            COST.data.total += elapsed;
        }
    }

    pub fn acc_lsm_put(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.put += elapsed;
            COST.lsm.total += elapsed;
        }
    }

    pub fn acc_lsm_put_wal(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.put_wal += elapsed;
        }
    }

    pub fn acc_lsm_put_compaction_minor(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.put_compaction_minor += elapsed;
        }
    }

    pub fn acc_lsm_put_compaction_major(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.put_compaction_major += elapsed;
        }
    }

    pub fn acc_lsm_sync(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.sync += elapsed;
            COST.lsm.total += elapsed;
        }
    }

    pub fn acc_lsm_sync_wal(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.sync_wal += elapsed;
        }
    }

    pub fn acc_lsm_sync_memtable(time: Instant) {
        unsafe {
            let elapsed = time.elapsed();
            COST.lsm.sync_memtable += elapsed;
        }
    }

    pub fn acc_data_amound(nblocks: usize) {
        unsafe {
            COST.amount.data += nblocks;
        }
    }

    pub fn acc_lsm_amound(nblocks: usize) {
        unsafe {
            COST.amount.lsm += nblocks;
        }
    }

    pub fn display() {
        unsafe {
            println!("\n========= SwornDisk Breakdown Cost =========");
            println!("{:?}", COST);
            println!(
                "Data cost : TxLsmTree cost = [ 1 : {:.3} ]",
                (COST.lsm.total.as_secs_f64()) / (COST.data.total.as_secs_f64())
            );
            println!(
                "Data cost internal: [ write: {:.2}%, sync: {:.2}% ]",
                (COST.data.write.as_secs_f64()) / (COST.data.total.as_secs_f64()) * 100.0,
                (COST.data.sync.as_secs_f64()) / (COST.data.total.as_secs_f64()) * 100.0,
            );
            println!(
                "TxLsmTree cost internal: [ put: {:.2}%, sync: {:.2}%, put_wal: {:.2}%, put_compaction_minor: {:.2}%, put_compaction_major: {:.2}%, sync_wal: {:.2}%, sync_memtable: {:.2}% ]",
                (COST.lsm.put.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.sync.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.put_wal.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.put_compaction_minor.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.put_compaction_major.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.sync_wal.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
                (COST.lsm.sync_memtable.as_secs_f64()) / (COST.lsm.total.as_secs_f64()) * 100.0,
            );
            println!(
                "Write Amplification Factor: {:.4}",
                (COST.amount.data + COST.amount.lsm + COST.amount.journal) as f64
                    / COST.amount.data as f64
            );
            println!("========= SwornDisk Breakdown Cost =========",);
        }
    }
}
