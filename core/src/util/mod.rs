//! Utilities.
mod bitmap;
mod crypto;
mod lazy_delete;
mod metrics;

pub use self::bitmap::BitMap;
pub use self::crypto::{Aead, RandomInit, Rng, Skcipher};
pub use self::lazy_delete::LazyDelete;
pub use self::metrics::{AmpType, AmplificationMetrics, LatencyMetrics, Metrics, ReqType};

/// Aligns `x` up to the next multiple of `align`.
pub(crate) const fn align_up(x: usize, align: usize) -> usize {
    ((x + align - 1) / align) * align
}

/// Aligns `x` down to the previous multiple of `align`.
pub(crate) const fn align_down(x: usize, align: usize) -> usize {
    (x / align) * align
}
