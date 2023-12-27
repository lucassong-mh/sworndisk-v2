// #![no_std]
#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(is_sorted)]
#![feature(let_chains)]
#![feature(negative_impls)]
#![feature(new_uninit)]
#![feature(slice_concat_trait)]
#![feature(slice_group_by)]

mod error;
mod layers;
mod os;
mod prelude;
mod tx;
mod util;

extern crate alloc;

pub use self::error::{Errno, Error};
pub use self::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef, BLOCK_SIZE};
pub use self::layers::disk::SwornDisk;
pub use self::os::{Aead, AeadIv, AeadKey, AeadMac, Rng};
pub use self::util::{Aead as _, Metrics, RandomInit, Rng as _};
