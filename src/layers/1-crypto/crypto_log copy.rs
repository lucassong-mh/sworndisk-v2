use super::{Iv, Key, Mac};
use crate::layers::bio::{BlockId, BlockLog, Buf, BufMut, BufRef, BLOCK_SIZE};
use crate::os::{Aead, Mutex, RwLock};
use crate::prelude::*;

use alloc::collections::VecDeque;
use core::any::Any;
use core::fmt::{self, Debug};
use core::mem::size_of;
use lending_iterator::LendingIterator;
use pod::Pod;
use serde::{Deserialize, Serialize};
use static_assertions::const_assert;

/// A cryptographically-protected log of user data blocks.
///
/// `CryptoLog<L>`, which is backed by an untrusted block log (`L`),
/// serves as a secure log file that supports random reads and append-only
/// writes of data blocks. `CryptoLog<L>` encrypts the data blocks and
/// protects them with a Merkle Hash Tree (MHT), which itself is also encrypted.
///
/// # Security
///
/// Each instance of `CryptoLog<L>` is assigned a randomly-generated root key
/// upon its creation. The root key is used to encrypt the root MHT block only.
/// Each new version of the root MHT block is encrypted with the same key, but
/// different random IVs. This arrangement ensures the confidentiality of
/// the root block.
///
/// After flushing a `CryptoLog<L>`, a new root MHT (as well as other MHT nodes)
/// shall be appended to the backend block log (`L`).
/// The metadata of the root MHT, including its position, encryption
/// key, IV, and MAC, must be kept by the user of `CryptoLog<L>` so that
/// he or she can use the metadata to re-open the `CryptoLog`.
/// The information contained in the metadata is sufficient to verify the
/// integrity and freshness of the root MHT node, and thus the whole `CryptoLog`.
///
/// Other MHT nodes as well as data nodes are encrypted with randomly-generated,
/// unique keys. Their metadata, including its position, encryption key, IV, and
/// MAC, are kept securely in their parent MHT nodes, which are also encrypted.
/// Thus, the confidentiality and integrity of non-root nodes are protected.
///
/// # Performance
///
/// Thanks to its append-only nature, `CryptoLog<L>` avoids MHT's high
/// performance overheads under the workload of random writes
/// due to "cascades of updates".
///
/// Behind the scene, `CryptoLog<L>` keeps a cache for nodes so that frequently
/// or lately accessed nodes can be found in the cache, avoiding the I/O
/// and decryption cost incurred when re-reading these nodes.
/// The cache is also used for buffering new data so that multiple writes to
/// individual nodes can be merged into a large write to the underlying block log.
/// Therefore, `CryptoLog<L>` is efficient for both reads and writes.
///
/// # Disk space
///
/// One consequence of using an append-only block log (`L`) as the backend is
/// that `CryptoLog<L>` cannot do in-place updates to existing MHT nodes.
/// This means the new version of MHT nodes are appended to the underlying block
/// log and the invalid blocks occupied by old versions are not reclaimed.
///
/// But lucky for us, this block reclamation problem is not an issue in practice.
/// This is because a `CryptoLog<L>` is created for one of the following two
/// use cases.
///
/// 1. Write-once-then-read-many. In this use case, all the content of a
/// `CryptoLog` is written in a single run.
/// Writing in a single run won't trigger any updates to MHT nodes and thus
/// no waste of disk space.
/// After the writing is done, the `CryptoLog` becomes read-only.
///
/// 2. Write-many-then-read-once. In this use case, the content of a
/// `CryptoLog` may be written in many runs. But the number of `CryptoLog`
/// under such workloads is limited and their lengths are also limited.
/// So the disk space wasted by such `CryptoLog` is bounded.
/// And after such `CryptoLog`s are done writing, they will be read once and
/// then discarded.
pub struct CryptoLog<L> {
    block_log: L,
    key: Key,
    root_meta: RwLock<Option<RootMhtMeta>>,
    append_buf: RwLock<AppendBuf>,
    cache: Arc<dyn NodeCache>,
    // append_flush_lock: Mutex<()>, // Used to prevent concurrent appends or flushes
}

type Lbid = BlockId; // Logical block position, in terms of user
type Pbid = BlockId; // Physical block position, in terms of underlying log

pub struct Mht {
    root: Option<(RootMhtMeta, Arc<MhtNode>)>,
    node_cache: Arc<dyn NodeCache>,
    append_buf: AppendBuf,
}

/// The metadata of the root MHT node of a `CryptoLog`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RootMhtMeta {
    pub pos: Pbid,
    pub mac: Mac,
    pub iv: Iv,
}

/// The Merkle-Hash Tree (MHT) node (internal).
/// It contains a header for node metadata and a bunch of entries for managing children nodes.
#[repr(C)]
#[derive(Clone, Copy, Pod)]
struct MhtNode {
    header: MhtNodeHeader,
    entries: [MhtNodeEntry; MHT_NBRANCHES],
}
const_assert!(size_of::<MhtNode>() <= BLOCK_SIZE);

/// The header contains metadata of the current MHT node.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod)]
struct MhtNodeHeader {
    // The height of the MHT whose root is this node
    height: u8,
    // The total number of valid data nodes covered by this node
    num_data_nodes: u32,
    // The number of valid children within this node
    num_valid_child: u16,
}

/// The entry of the MHT node, which contains the
/// metadata of the child MHT/data node.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod)]
struct MhtNodeEntry {
    pos: Pbid,
    key: Key,
    mac: Mac,
}

// Number of branches of one MHT node. (102 for now)
const MHT_NBRANCHES: usize = (BLOCK_SIZE - size_of::<MhtNodeHeader>()) / size_of::<MhtNodeEntry>();

/// The data node (leaf). It contains a block of data.
#[repr(C)]
#[derive(Clone, Copy, Pod)]
struct DataNode([u8; BLOCK_SIZE]);

/// The node cache used by `CryptoLog`. User-defined node cache
/// can achieve TX-awareness.
pub trait NodeCache {
    /// Gets an owned value from cache corresponding to the position.
    fn get(&self, pos: Pbid) -> Option<Arc<dyn Any + Send + Sync>>;

    /// Puts a position-value pair into cache. If the value of that position
    /// already exists, updates it and returns the old value. Otherwise, `None` is returned.
    fn put(
        &self,
        pos: Pbid,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>>;
}

impl<L: BlockLog> CryptoLog<L> {
    // Buffer capacity for appended data nodes.
    const APPEND_BUF_CAPACITY: usize = 1024;

    /// Creates a new `CryptoLog`.
    ///
    /// A newly-created instance won't occupy any space on the `block_log`
    /// until the first flush, which triggers writing the root MHT node.
    pub fn new(block_log: L, key: Key, cache: Arc<dyn NodeCache>) -> Self {
        let append_pos = 0 as Lbid;
        Self {
            block_log,
            key,
            root_meta: RwLock::new(None),
            cache,
            append_buf: RwLock::new(AppendBuf::new(append_pos, Self::APPEND_BUF_CAPACITY)),
            // append_flush_lock: Mutex::new(()),
        }
    }

    /// Opens an existing `CryptoLog` backed by a `block_log`.
    ///
    /// The given key and the metadata of the root MHT are sufficient to
    /// load and verify the root node of the `CryptoLog`.
    pub fn open(
        block_log: L,
        key: Key,
        root_meta: RootMhtMeta,
        cache: Arc<dyn NodeCache>,
    ) -> Result<Self> {
        let append_pos: Lbid = {
            let root_node = if let Some(node) = cache.get(root_meta.pos) {
                node.downcast::<MhtNode>().map_err(|_| {
                    Error::with_msg(InvalidArgs, "cache node downcasts to root MHT node failed")
                })?
            } else {
                let mut cipher = Buf::alloc(1)?;
                let mut plain = Buf::alloc(1)?;
                block_log.read(root_meta.pos, cipher.as_mut())?;
                Aead::new().decrypt(
                    cipher.as_slice(),
                    &key,
                    &root_meta.iv,
                    &[],
                    &root_meta.mac,
                    plain.as_mut_slice(),
                )?;
                Arc::new(MhtNode::from_bytes(plain.as_slice()))
            };
            let append_pos = root_node.header.num_data_nodes as Lbid;
            cache.put(root_meta.pos, root_node);
            append_pos
        };
        Ok(Self {
            block_log,
            key,
            root_meta: RwLock::new(Some(root_meta)),
            cache,
            append_buf: RwLock::new(AppendBuf::new(append_pos, Self::APPEND_BUF_CAPACITY)),
            // append_flush_lock: Mutex::new(()),
        })
    }

    /// Gets the root key.
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Gets the metadata of the root MHT node.
    ///
    /// Returns `None` if there hasn't been any appends or flush.
    pub fn root_meta(&self) -> Option<RootMhtMeta> {
        *self.root_meta.read()
    }

    /// Gets the number of data nodes (blocks).
    pub fn nblocks(&self) -> usize {
        let root_guard = self.root_meta.read();
        let append_buf_nblocks = self.append_buf.read().num_append();
        let nblocks = append_buf_nblocks
            + root_guard.map_or(0, |root_meta| {
                if let Ok(root_node) = self.root_mht_node(&root_meta) {
                    root_node.header.num_data_nodes as usize
                } else {
                    0
                }
            });
        nblocks
    }

    /// Reads one or multiple data blocks at a specified position.
    pub fn read(&self, mut pos: Lbid, mut buf: BufMut) -> Result<()> {
        let root_node = self.root_mht_node(
            &self
                .root_meta
                .read()
                .ok_or(Error::with_msg(NotFound, "root MHT node not found"))?,
        )?;

        let (mut lookup_mht_node, mut nth_entry) = (None, 0);
        let mut iter_mut = buf.iter_mut();
        while let Some(mut block_buf) = iter_mut.next() {
            // Read from append buffer first
            if let Some(data_node) = self.append_buf.read().get_data(pos) {
                block_buf.as_mut_slice().copy_from_slice(&data_node.0);
                continue;
            }

            if lookup_mht_node.is_none() {
                // Locate the target last-level MHT node given the position
                let (node, nth) =
                    self.locate_last_level_mht_node_with_nth_entry(pos, &root_node)?;
                debug_assert_eq!(node.header.height, 1);
                let _ = lookup_mht_node.insert(node);
                nth_entry = nth;
            }
            let target_mht_node = lookup_mht_node.as_ref().unwrap();

            // Read the target data node
            let target_entry = &target_mht_node.entries[nth_entry];
            self.read_data_node(target_entry, block_buf)?;

            nth_entry += 1;
            pos += 1;
            // Should lookup the next target if current lookup reaches the MHT node's end
            if nth_entry == target_mht_node.header.num_valid_child as _ {
                let _ = lookup_mht_node.take();
            }
        }
        Ok(())
    }

    // Given a position, locates the last-level MHT node (height always equals 1)
    // and the inner index of entries to the target data node.
    fn locate_last_level_mht_node_with_nth_entry(
        &self,
        pos: Lbid,
        root_node: &Arc<MhtNode>,
    ) -> Result<(Arc<MhtNode>, usize)> {
        if root_node.header.num_data_nodes <= pos as _ {
            return_errno_with_msg!(InvalidArgs, "read out of bound");
        }

        let mut offset = pos;
        let mut lookup_node = root_node.clone();
        // Loop until we reach to the leaf
        while lookup_node.header.height != 1 {
            // Locate the target entry of current node and update the offset
            let nth_entry = offset / MHT_NBRANCHES;
            debug_assert!(nth_entry < lookup_node.header.num_valid_child as _);
            let entry = lookup_node.entries[nth_entry];
            offset -= nth_entry * (lookup_node.max_num_data_nodes() / MHT_NBRANCHES);

            // Lookup in the target child MHT node
            lookup_node =
                self.read_mht_node(entry.pos, &entry.key, &entry.mac, &Iv::new_zeroed())?;
        }

        Ok((lookup_node, offset))
    }

    /// Appends one or multiple data blocks at the end.
    pub fn append(&self, buf: BufRef) -> Result<()> {
        let mut root_guard = self.root_meta.write();

        let mut append_buf = self.append_buf.write();
        for block_buf in buf.iter() {
            let data_node = {
                let mut node = DataNode::new_uninit();
                node.0.copy_from_slice(block_buf.as_slice());
                Arc::new(node)
            };
            append_buf.append_data(data_node);

            if append_buf.is_full() {
                drop(append_buf);

                self.flush_append_buf()?;
                append_buf = self.append_buf.write();
            }
        }
        Ok(())
    }

    /// Ensures that all new data are persisted.
    ///
    /// Each successful flush triggers writing a new version of the root MHT
    /// node to the underlying block log. The metadata of the latest root MHT
    /// can be obtained via the `root_meta` method.
    pub fn flush(&self) -> Result<()> {
        let root_guard = self.root_meta.write();

        self.flush_append_buf()?;

        self.block_log.flush()
    }

    // Flushes all buffered data nodes to the underlying block log. A new root MHT node
    // would be built and the whole MHT is updated based on the newly-appended data.
    fn flush_append_buf(&self) -> Result<()> {
        // let root_guard = self.root_meta.lock();

        let data_nodes = self.append_buf.read().all_buffered_data();
        if data_nodes.is_empty() {
            return Ok(());
        }

        let new_root_meta = self.build_mht(&data_nodes)?;
        let _ = self.root_meta.lock().insert(new_root_meta);

        self.append_buf.write().clear();
        Ok(())
    }

    // Builds a MHT given a bunch of data nodes (along with the incomplete MHT nodes from previous),
    // On success, returns the metadata of the generated root node.
    fn build_mht(
        &self,
        data_nodes: &Vec<Arc<DataNode>>,
        root_node_opt: Option<Arc<MhtNode>>,
    ) -> Result<RootMhtMeta> {
        // let root_node_opt = self.root_mht_node().ok();
        let root_height = root_node_opt
            .as_ref()
            .map_or_else(|| 0, |node| node.header.height);

        // Collect incomplete MHT nodes first
        let mut incomplete_node_queue = VecDeque::new();
        if let Some(root_node) = root_node_opt {
            self.collect_incomplete_mht_nodes(&root_node, &mut incomplete_node_queue)?;
        }

        self.do_build_mht(data_nodes, incomplete_node_queue, root_height)
    }

    fn do_build_mht(
        &self,
        data_nodes: &Vec<Arc<DataNode>>,
        mut incomplete_node_queue: VecDeque<Arc<MhtNode>>,
        root_height: u8,
    ) -> Result<RootMhtMeta> {
        let need_incomplete_nodes_for_building = !incomplete_node_queue.is_empty();
        let mut height = 1;

        // Prepare two queues for each iteration (each level)
        let mut curr_level_entries: VecDeque<MhtNodeEntry> = {
            let mut entries = VecDeque::with_capacity(data_nodes.len());
            // Append newly data nodes and collect corresponding MHT node entries.
            for data_node in data_nodes {
                entries.push_back(self.append_data_node(&data_node)?);
            }
            entries
        };
        let mut curr_level_num_data_nodes: VecDeque<u32> =
            curr_level_entries.iter().map(|_| 1_u32).collect();

        if need_incomplete_nodes_for_building {
            update_level_queues_with_incomplete_nodes(
                &mut incomplete_node_queue,
                &mut curr_level_entries,
                &mut curr_level_num_data_nodes,
                height,
            );
        }

        // Loop to construct the MHT hierarchically, from bottom to top
        let new_root_node = 'outer: loop {
            let level_size = curr_level_entries.len();
            // Each loop builds a whole level of MHT nodes
            for idx in 0..level_size {
                let is_complete = (idx + 1) % MHT_NBRANCHES == 0;
                if !is_complete && idx != level_size - 1 {
                    // Not a complete MHT node, or not the last entry of current level
                    continue;
                }

                let new_mht_node = {
                    let num_valid_child = if is_complete {
                        MHT_NBRANCHES
                    } else {
                        // Last incomplete node of current level
                        level_size % MHT_NBRANCHES
                    };
                    let mut num_data_nodes = 0;
                    let entries = array_init::array_init(|i| {
                        if i < num_valid_child {
                            num_data_nodes += curr_level_num_data_nodes.pop_front().unwrap();
                            curr_level_entries.pop_front().unwrap()
                        } else {
                            // Padding invalid entries to the rest
                            MhtNodeEntry::new_uninit()
                        }
                    });
                    Arc::new(MhtNode {
                        header: MhtNodeHeader {
                            height,
                            num_data_nodes,
                            num_valid_child: num_valid_child as _,
                        },
                        entries,
                    })
                };

                // If there is only one MHT node built on current level, and it's
                // higher than previous root, then it becomes the new root node
                if height >= root_height && level_size <= MHT_NBRANCHES {
                    break 'outer new_mht_node;
                }

                let new_entry: MhtNodeEntry = self.append_mht_node(&new_mht_node)?;
                curr_level_entries.push_back(new_entry);
                curr_level_num_data_nodes.push_back(new_mht_node.header.num_data_nodes);
            }
            height += 1;

            if need_incomplete_nodes_for_building {
                update_level_queues_with_incomplete_nodes(
                    &mut incomplete_node_queue,
                    &mut curr_level_entries,
                    &mut curr_level_num_data_nodes,
                    height,
                );
            }
        };

        return self.append_root_mht_node(&new_root_node);

        // If there are incomplete nodes from previous, we need to update the two queues
        fn update_level_queues_with_incomplete_nodes(
            incomplete_node_queue: &mut VecDeque<Arc<MhtNode>>,
            level_entries: &mut VecDeque<MhtNodeEntry>,
            level_num_data_nodes: &mut VecDeque<u32>,
            level_height: u8,
        ) {
            // Incompelete nodes only involve in the building for the same level
            if incomplete_node_queue.is_empty()
                || incomplete_node_queue.back().unwrap().header.height != level_height
            {
                return;
            }

            let incomplete_node = incomplete_node_queue.pop_back().unwrap();
            let header = &incomplete_node.header;
            // Only collect the complete entries from the incomplete node, ignore the last incomplete entry (if exists)
            // since its corresponding node already been processed from previous building round
            let num_complete_entries: usize =
                if header.num_data_nodes as usize % MHT_NBRANCHES == 0 || header.height == 1 {
                    header.num_valid_child as _
                } else {
                    (header.num_valid_child - 1) as _
                };
            let num_data_nodes_each_entry = incomplete_node.max_num_data_nodes() / MHT_NBRANCHES;

            for i in (0..num_complete_entries).rev() {
                // Put them to the front of the queue for later building
                level_entries.push_front(incomplete_node.entries[i]);
                level_num_data_nodes.push_front(num_data_nodes_each_entry as _);
            }
        }
    }

    /// Collects incomplete (not fully filled) MHT nodes from the given root node.
    fn collect_incomplete_mht_nodes(
        &self,
        root_node: &Arc<MhtNode>,
        res_queue: &mut VecDeque<Arc<MhtNode>>,
    ) -> Result<()> {
        let root_height = root_node.header.height;
        let mut lookup_node = root_node.clone();
        loop {
            let header = &lookup_node.header;
            // Loop until we reach to the last complete MHT node,
            // or to the bottom level of MHT nodes
            if !lookup_node.is_incomplete() || header.height == 1 {
                if header.height == root_height {
                    // Root node must be collected for later building,
                    // no matter if it is incomplete or not
                    res_queue.push_back(lookup_node);
                }
                break;
            }
            res_queue.push_back(lookup_node.clone());

            // Next lookup node must at the end of the children valid nodes
            let entry: &MhtNodeEntry = &lookup_node.entries[(header.num_valid_child - 1) as usize];
            lookup_node =
                self.read_mht_node(entry.pos, &entry.key, &entry.mac, &Iv::new_zeroed())?;
        }
        Ok(())
    }

    fn append_root_mht_node(&self, node: &Arc<MhtNode>) -> Result<RootMhtMeta> {
        let (cipher, mac, iv) = {
            let plain = node.as_bytes();
            let mut cipher = Buf::alloc(1)?;
            let iv = Iv::random();
            let mac =
                Aead::new().encrypt(&plain, &self.key, &iv, &[], &mut cipher.as_mut_slice())?;
            (cipher, mac, iv)
        };

        let pos = self.block_log.append(cipher.as_ref())?;
        self.cache.put(pos, node.clone());
        Ok(RootMhtMeta { pos, mac, iv })
    }

    fn append_mht_node(&self, node: &Arc<MhtNode>) -> Result<MhtNodeEntry> {
        let (cipher, key, mac) = {
            let plain = node.as_bytes();
            let mut cipher = Buf::alloc(1)?;
            let key = Key::random();
            let mac = Aead::new().encrypt(
                &plain,
                &key,
                &Iv::new_zeroed(),
                &[],
                &mut cipher.as_mut_slice(),
            )?;
            (cipher, key, mac)
        };

        let pos = self.block_log.append(cipher.as_ref())?;
        self.cache.put(pos, node.clone());
        Ok(MhtNodeEntry { pos, key, mac })
    }

    fn append_data_node(&self, node: &DataNode) -> Result<MhtNodeEntry> {
        let (cipher, key, mac) = {
            let mut cipher = Buf::alloc(1)?;
            let key = Key::random();
            let mac = Aead::new().encrypt(
                &node.0,
                &key,
                &Iv::new_zeroed(),
                &[],
                &mut cipher.as_mut_slice(),
            )?;
            (cipher, key, mac)
        };

        let pos = self.block_log.append(cipher.as_ref())?;
        Ok(MhtNodeEntry { pos, key, mac })
    }

    fn root_mht_node(&self, root_meta: &RootMhtMeta) -> Result<Arc<MhtNode>> {
        self.read_mht_node(root_meta.pos, &self.key, &root_meta.mac, &root_meta.iv)
    }

    fn read_mht_node(&self, pos: Pbid, key: &Key, mac: &Mac, iv: &Iv) -> Result<Arc<MhtNode>> {
        if let Some(node) = self.cache.get(pos) {
            return Ok(node.downcast::<MhtNode>().map_err(|_| {
                Error::with_msg(InvalidArgs, "cache node downcasts to MHT node failed")
            })?);
        }

        let mht_node = {
            let mut cipher = Buf::alloc(1)?;
            let mut plain = Buf::alloc(1)?;
            self.block_log.read(pos, cipher.as_mut())?;
            Aead::new().decrypt(cipher.as_slice(), key, iv, &[], mac, plain.as_mut_slice())?;
            Arc::new(MhtNode::from_bytes(plain.as_slice()))
        };

        self.cache.put(pos, mht_node.clone());
        Ok(mht_node)
    }

    fn read_data_node(&self, entry: &MhtNodeEntry, mut buf: BufMut) -> Result<()> {
        let mut cipher = Buf::alloc(1)?;
        self.block_log.read(entry.pos, cipher.as_mut())?;
        Aead::new().decrypt(
            cipher.as_slice(),
            &entry.key,
            &Iv::new_zeroed(),
            &[],
            &entry.mac,
            buf.as_mut_slice(),
        )
    }

    pub fn display_mht(&self) {
        println!("{:?}", CryptoLogDisplayer(self));
    }
}

impl MhtNode {
    pub fn is_incomplete(&self) -> bool {
        self.header.num_data_nodes != self.max_num_data_nodes() as _
    }

    pub fn max_num_data_nodes(&self) -> usize {
        MHT_NBRANCHES.pow(self.header.height as _)
    }
}

/// A buffer that contains appended data.
struct AppendBuf {
    data_queue: Vec<Arc<DataNode>>,
    append_pos: Lbid,
    capacity: usize,
}

impl AppendBuf {
    pub fn new(append_pos: Lbid, capacity: usize) -> Self {
        Self {
            data_queue: Vec::new(),
            append_pos,
            capacity,
        }
    }

    pub fn append_data(&mut self, node: Arc<DataNode>) {
        self.data_queue.push(node);
    }

    pub fn get_data(&self, pos: Lbid) -> Option<Arc<DataNode>> {
        let append_pos = self.append_pos;
        if pos < append_pos || pos - append_pos >= self.data_queue.len() {
            return None;
        }
        Some(self.data_queue[pos - append_pos].clone())
    }

    pub fn num_append(&self) -> usize {
        self.data_queue.len()
    }

    pub fn is_full(&self) -> bool {
        // Returns whether the buffer is at capacity
        self.data_queue.len() >= self.capacity
    }

    pub fn all_buffered_data(&self) -> Vec<Arc<DataNode>> {
        self.data_queue.iter().map(|data| data.clone()).collect()
    }

    pub fn clear(&mut self) {
        self.append_pos += self.num_append();
        self.data_queue.clear();
    }
}

impl<L> Debug for CryptoLog<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CryptoLog")
            .field("key", &self.key)
            .field("root_mht_meta", &self.root_meta.lock())
            .field("append_buf_nblocks", &self.append_buf.read().num_append())
            .finish()
    }
}

impl Debug for MhtNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MhtNode")
            .field("height", &self.header.height)
            .field("num_data_nodes", &self.header.num_data_nodes)
            .field("num_valid_child", &self.header.num_valid_child)
            .finish()
    }
}

impl Debug for DataNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataNode")
            .field("first 16 bytes", &&self.0[..16])
            .finish()
    }
}

struct CryptoLogDisplayer<'a, L>(&'a CryptoLog<L>);

impl<'a, L: BlockLog> Debug for CryptoLogDisplayer<'a, L> {
    // A heavy implementation to display the whole MHT.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("CryptoLog");

        // Display root MHT node
        let root_meta = self.0.root_meta();
        debug_struct.field("\nroot_meta", &root_meta);
        if root_meta.is_none() {
            return debug_struct.finish();
        }
        let root_mht_node = self.0.root_mht_node().unwrap();
        debug_struct.field("\n-> root_mht_node", &root_mht_node);
        let mut height = root_mht_node.header.height;
        if height == 1 {
            return debug_struct.finish();
        }

        // Display internal MHT nodes level-by-level
        let mut level_entries: VecDeque<MhtNodeEntry> = root_mht_node
            .entries
            .into_iter()
            .take(root_mht_node.header.num_valid_child as _)
            .collect();
        'outer: loop {
            let level_size = level_entries.len();
            for _ in 0..level_size {
                let entry = level_entries.pop_front().unwrap();
                let node = self
                    .0
                    .read_mht_node(entry.pos, &entry.key, &entry.mac, &Iv::new_zeroed())
                    .unwrap();
                debug_struct.field("\n node_entry", &entry);
                debug_struct.field("\n -> mht_node", &node);
                for i in 0..node.header.num_valid_child {
                    level_entries.push_back(node.entries[i as usize]);
                }
            }
            height -= 1;
            if height == 1 {
                break 'outer;
            }
        }
        debug_struct.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::MemLog;

    struct NoCache;
    impl NodeCache for NoCache {
        fn get(&self, _pos: Pbid) -> Option<Arc<dyn Any + Send + Sync>> {
            None
        }
        fn put(
            &self,
            _pos: Pbid,
            _value: Arc<dyn Any + Send + Sync>,
        ) -> Option<Arc<dyn Any + Send + Sync>> {
            None
        }
    }

    fn create_crypto_log() -> Result<CryptoLog<MemLog>> {
        let mem_log = MemLog::create(4 * 1024)?;
        let key = Key::random();
        let cache = Arc::new(NoCache {});
        Ok(CryptoLog::new(mem_log, key, cache))
    }

    #[test]
    fn crypto_log_fns() -> Result<()> {
        let log = create_crypto_log()?;
        let append_cnt = MHT_NBRANCHES - 1;
        let mut buf = Buf::alloc(1)?;
        for i in 0..append_cnt {
            buf.as_mut_slice().fill(i as _);
            log.append(buf.as_ref())?;
        }
        log.flush()?;
        println!("{:?}", log);
        log.display_mht();

        let content = 5u8;
        buf.as_mut_slice().fill(content);
        log.append(buf.as_ref())?;
        log.flush()?;
        log.display_mht();
        log.append(buf.as_ref())?;
        log.flush()?;
        log.display_mht();

        let (root_meta, root_node) = (log.root_meta().unwrap(), log.root_mht_node()?);
        assert_eq!(root_meta.pos, 107);
        assert_eq!(root_node.header.height, 2);
        assert_eq!(root_node.header.num_data_nodes as usize, append_cnt + 2);
        assert_eq!(root_node.header.num_valid_child, 2);

        log.read(5 as BlockId, buf.as_mut())?;
        assert_eq!(buf.as_slice(), [content; BLOCK_SIZE]);
        let mut buf = Buf::alloc(2)?;
        log.read((MHT_NBRANCHES - 1) as BlockId, buf.as_mut())?;
        assert_eq!(buf.as_slice(), [content; 2 * BLOCK_SIZE]);
        Ok(())
    }

    #[test]
    fn write_once_read_many() -> Result<()> {
        let log = create_crypto_log()?;
        let append_cnt = 1024_usize;
        let mut buf = Buf::alloc(append_cnt)?;
        for i in 0..append_cnt {
            buf.as_mut_slice()[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE].fill(i as _);
        }
        log.append(buf.as_ref())?;
        log.flush()?;
        log.display_mht();

        let mut buf = Buf::alloc(1)?;
        for i in (0..append_cnt).rev() {
            log.read(i as Lbid, buf.as_mut())?;
            assert_eq!(buf.as_slice(), [i as u8; BLOCK_SIZE]);
        }
        Ok(())
    }

    #[test]
    fn write_many_read_once() -> Result<()> {
        let log = create_crypto_log()?;
        let append_cnt = 1024_usize;
        let mut buf = Buf::alloc(1)?;
        for i in 0..append_cnt {
            buf.as_mut_slice().fill(i as _);
            log.append(buf.as_ref())?;
        }
        log.flush()?;
        log.display_mht();

        let mut buf = Buf::alloc(append_cnt)?;
        log.read(0 as Lbid, buf.as_mut())?;
        for i in 0..append_cnt {
            assert_eq!(
                buf.as_slice()[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE],
                [i as u8; BLOCK_SIZE]
            );
        }
        Ok(())
    }
}

pub fn old_build(&self) -> Vec<Arc<MhtNode>> {
    let built_len = align_up(self.level_entries.len(), MHT_NBRANCHES) / MHT_NBRANCHES;
    let mut new_mht_nodes = Vec::with_capacity(built_len);

    let max_data_nodes_per_node = MhtNode::max_num_data_nodes(self.height);
    let num_data_nodes_last_node = {
        let incomplete_nodes = self.total_data_nodes % max_data_nodes_per_node;
        if incomplete_nodes == 0 {
            max_data_nodes_per_node
        } else {
            incomplete_nodes
        }
    };

    let mut nth = 1;
    let all_level_entries: Vec<&MhtNodeEntry> =
        if let Some(pre_node) = self.previous_built_node.as_ref() {
            pre_node
                .entries
                .iter()
                .take(pre_node.num_complete_children())
                .chain(self.level_entries.iter())
                .collect()
        } else {
            self.level_entries.iter().collect()
        };
    println!("all_entries_len: {}", all_level_entries.len(),);
    // TODO: Abstract this to a `impl Iterator`
    for entries_per_node in all_level_entries.chunks(MHT_NBRANCHES) {
        println!(
            "nth: {}, built_len: {}, entries_len: {}",
            nth,
            built_len,
            entries_per_node.len()
        );
        if nth == built_len {
            println!("222");
            let last_new_node = self.build_last_node(entries_per_node);
            new_mht_nodes.push(last_new_node);
            continue;
        }

        let num_valid_entries = entries_per_node.len();
        let num_data_nodes = if nth == built_len {
            num_data_nodes_last_node
        } else {
            max_data_nodes_per_node
        };
        let new_mht_node = Arc::new(MhtNode {
            header: MhtNodeHeader {
                height: self.height,
                num_data_nodes: num_data_nodes as _,
                num_valid_entries: num_valid_entries as _,
            },
            entries: array_init::array_init(|i| {
                if i < num_valid_entries {
                    *entries_per_node[i]
                } else {
                    // Padding invalid entries to the rest
                    MhtNodeEntry::new_uninit()
                }
            }),
        });
        new_mht_nodes.push(new_mht_node);
        nth += 1;
    }
    println!("333");

    new_mht_nodes
}
