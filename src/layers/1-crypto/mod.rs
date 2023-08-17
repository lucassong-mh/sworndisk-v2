//! The layer of cryptographical constructs.

mod crypto_blob;
mod crypto_chain;
mod crypto_log;

pub use self::crypto_blob::CryptoBlob;
pub use self::crypto_chain::CryptoChain;
pub use self::crypto_log::{CryptoLog, NodeCache, RootMhtMeta};

pub type Key = crate::os::AeadKey;
pub type Iv = crate::os::AeadIv;
pub type Mac = crate::os::AeadMac;
pub type VersionId = u64;
