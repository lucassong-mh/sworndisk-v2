use sworndisk_v2::*;

use spin::Mutex;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;

pub(crate) type Result<T> = core::result::Result<T, Error>;

#[derive(Clone)]
struct FileAsDisk {
    file: Arc<Mutex<File>>,
}

impl FileAsDisk {
    pub fn new(nblocks: usize) -> Self {
        let path = "sworndisk_bench";
        let file = File::create(path).unwrap();
        file.set_len((nblocks * BLOCK_SIZE) as _).unwrap();
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }
}

impl BlockSet for FileAsDisk {
    fn read(&self, pos: BlockId, buf: BufMut) -> Result<()> {
        unimplemented!()
    }

    fn write(&self, pos: BlockId, buf: BufRef) -> Result<()> {
        self.file.lock().seek(SeekFrom::Start(pos as _)).unwrap();
        self.file.lock().write(buf.as_slice()).unwrap();
        let plain = Buf::alloc(1).unwrap();
        let mut cipher = Buf::alloc(1).unwrap();
        Aead::new()
            .encrypt(
                plain.as_slice(),
                &AeadKey::default(),
                &AeadIv::default(),
                &[],
                cipher.as_mut_slice(),
            )
            .unwrap();
        Ok(())
    }

    fn subset(&self, range: std::ops::Range<BlockId>) -> Result<Self>
    where
        Self: Sized,
    {
        debug_assert!(range.len() < self.nblocks());
        // static SUBSET_CTR: AtomicU32 = AtomicU32::new(1);
        // let path = format!(
        //     "{:?}-{:?}",
        //     range,
        //     SUBSET_CTR.fetch_add(1, std::sync::atomic::Ordering::Release)
        // );
        // let subset = Self::new(range.len());
        // Ok(subset)
        Ok(self.clone())
    }

    fn flush(&self) -> Result<()> {
        self.file.lock().sync_all().unwrap();
        Ok(())
    }

    fn nblocks(&self) -> usize {
        self.file.lock().metadata().unwrap().len() as _
    }
}

fn main() {
    let nblocks = 32 * 1024;
    let wcnt = 1024;

    let start = Instant::now();
    let file_disk = FileAsDisk::new(nblocks);
    let sworndisk = SwornDisk::create(file_disk, AeadKey::default()).unwrap();
    let buf = Buf::alloc(1).unwrap();
    for i in 0..wcnt {
        sworndisk.write(i as BlockId, buf.as_ref()).unwrap();
    }
    sworndisk.sync().unwrap();
    let duration = start.elapsed();
    println!("sworndisk takes {:?}", duration);

    let start = Instant::now();
    let file_disk = FileAsDisk::new(nblocks);
    let buf = Buf::alloc(1).unwrap();
    for i in 0..wcnt {
        file_disk.write(i as BlockId, buf.as_ref()).unwrap();
    }
    file_disk.flush().unwrap();
    let duration = start.elapsed();
    println!("filedisk takes {:?}", duration);
}
