//! Occlum specific implementations.

use crate::prelude::*;

use sgx_rand::{thread_rng, Rng};
use sgx_tcrypto::{rsgx_aes_ctr_decrypt, rsgx_aes_ctr_encrypt};
use sgx_tcrypto::{rsgx_rijndael128GCM_decrypt, rsgx_rijndael128GCM_encrypt};
use sgx_types::sgx_status_t;
use std::alloc::{alloc, dealloc, Layout};

/// Reuse lock implementation of crate spin.
pub use spin::{
    Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockUpgradableGuard, RwLockWriteGuard,
};

/// Unique ID for the OS thread.
#[derive(PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Tid(sgx_libc::pthread_t);

/// A struct to get the current thread id.
pub struct CurrentThread;

impl CurrentThread {
    pub fn id() -> Tid {
        // SAFETY: calling extern "C" to get the thread_id is safe here.
        Tid(unsafe { sgx_libc::pthread_self() })
    }
}

pub const PAGE_SIZE: usize = 0x1000;

struct PageAllocator;

impl PageAllocator {
    /// Allocate memory buffer with specific size.
    ///
    /// The `len` indicates the number of pages.
    fn alloc(len: usize) -> Option<NonNull<u8>> {
        if len == 0 {
            return None;
        }

        // SAFETY: the `count` is non-zero, then the `Layout` has
        // non-zero size, so it's safe.
        unsafe {
            let layout =
                alloc::alloc::Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            let ptr = alloc::alloc::alloc(layout);
            NonNull::new(ptr)
        }
    }

    /// Deallocate memory buffer at the given `ptr` and `len`.
    ///
    /// # Safety
    ///
    /// The caller should make sure that:
    /// * `ptr` must denote the memory buffer currently allocated via
    ///   `PageAllocator::alloc`,
    ///
    /// * `len` must be the same size that was used to allocate the
    ///   memory buffer.
    unsafe fn dealloc(ptr: *mut u8, len: usize) {
        // SAFETY: the caller should pass valid `ptr` and `len`.
        unsafe {
            let layout =
                alloc::alloc::Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            alloc::alloc::dealloc(ptr, layout)
        }
    }
}

/// A struct for `PAGE_SIZE` aligned memory buffer.
pub struct Pages {
    ptr: NonNull<u8>,
    len: usize,
    _p: PhantomData<[u8]>,
}

// SAFETY: `Pages` owns the memory buffer, so it can be safely
// transferred across threads.
unsafe impl Send for Pages {}

impl Pages {
    /// Allocate specific number of pages.
    pub fn alloc(len: usize) -> Result<Self> {
        let ptr = PageAllocator::alloc(len).ok_or(Error::with_msg(
            Errno::OutOfMemory,
            "page allocation failed",
        ))?;
        Ok(Self {
            ptr,
            len,
            _p: PhantomData,
        })
    }

    /// Return the number of pages.
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Pages {
    fn drop(&mut self) {
        // SAFETY: `ptr` is `NonNull` and allocated by `PageAllocator::alloc`
        // with the same size of `len`, so it's valid and safe.
        unsafe { PageAllocator::dealloc(self.ptr.as_mut(), self.len) }
    }
}

impl core::ops::Deref for Pages {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

impl core::ops::DerefMut for Pages {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

/// A random number generator.
pub struct Rng;

impl crate::util::Rng for Rng {
    fn new(_seed: &[u8]) -> Self {
        Self
    }

    fn fill_bytes(&self, dest: &mut [u8]) -> Result<()> {
        thread_rng().fill_bytes(dest);
        Ok(())
    }
}

/// A macro to define byte_array_types used by `Aead` or `Skcipher`.
macro_rules! new_byte_array_type {
    ($name:ident, $n:expr) => {
        #[repr(C)]
        #[derive(Copy, Clone, Pod, Debug, Default, Deserialize, Serialize)]
        pub struct $name([u8; $n]);

        impl core::ops::Deref for $name {
            type Target = [u8];

            fn deref(&self) -> &Self::Target {
                self.0.as_slice()
            }
        }

        impl core::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.0.as_mut_slice()
            }
        }

        impl crate::util::RandomInit for $name {
            fn random() -> Self {
                use crate::util::Rng;

                let mut result = Self::default();
                let rng = self::Rng::new(&[]);
                rng.fill_bytes(&mut result).unwrap_or_default();
                result
            }
        }
    };
}

const AES_GCM_KEY_SIZE: usize = 16;
const AES_GCM_IV_SIZE: usize = 12;
const AES_GCM_MAC_SIZE: usize = 16;

new_byte_array_type!(AeadKey, AES_GCM_KEY_SIZE);
new_byte_array_type!(AeadIv, AES_GCM_IV_SIZE);
new_byte_array_type!(AeadMac, AES_GCM_MAC_SIZE);

/// An `AEAD` cipher.
pub struct Aead;

impl Aead {
    /// Construct an `Aead` instance.
    pub fn new() -> Self {
        Self
    }
}

impl crate::util::Aead for Aead {
    type Key = AeadKey;
    type Iv = AeadIv;
    type Mac = AeadMac;

    fn encrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        aad: &[u8],
        output: &mut [u8],
    ) -> Result<Self::Mac> {
        let mut mac = AeadMac::default();

        rsgx_rijndael128GCM_encrypt(key, input, iv, aad, output, &mut mac)
            .map_err(|_| Error::new(Errno::EncryptFailed))?;

        Ok(mac)
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        aad: &[u8],
        mac: &Self::Mac,
        output: &mut [u8],
    ) -> Result<()> {
        rsgx_rijndael128GCM_decrypt(key, input, iv, aad, mac, output)
            .map_err(|_| Error::new(Errno::DecryptFailed))?;

        Ok(())
    }
}

const AES_CTR_KEY_SIZE: usize = 16;
const AES_CTR_IV_SIZE: usize = 16;

new_byte_array_type!(SkcipherKey, AES_CTR_KEY_SIZE);
new_byte_array_type!(SkcipherIv, AES_CTR_IV_SIZE);

/// A symmetric key cipher.
pub struct Skcipher;

impl Skcipher {
    /// Construct a `Skcipher` instance.
    pub fn new() -> Self {
        Self
    }
}

impl crate::util::Skcipher for Skcipher {
    type Key = SkcipherKey;
    type Iv = SkcipherIv;

    fn encrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        output: &mut [u8],
    ) -> Result<()> {
        let ctr_inc_bits = 128u32;
        rsgx_aes_ctr_encrypt(key, input, iv, ctr_inc_bits, output)?;

        Ok(())
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        output: &mut [u8],
    ) -> Result<()> {
        let ctr_inc_bits = 128u32;
        rsgx_aes_ctr_decrypt(key, input, iv, ctr_inc_bits, output)?;

        Ok(())
    }
}
