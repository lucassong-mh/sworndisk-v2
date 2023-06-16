mod crypto_blob;
// mod crypto_chain;
mod crypto_log;

pub use self::crypto_blob::CryptoBlob;
// pub use self::crypto_chain::CryptoChain;
pub use self::crypto_log::CryptoLog;

pub type Key = [u8; 16];
pub type Mac = [u8; 16];
pub type Iv = [u8; 12];
