// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Symmetric Key Cipher.
//!
//! C header: [`include/crypto/skcipher.h`](include/crypto/skcipher.h)

use core::ptr::{addr_of_mut, NonNull};
use kernel::{
    bindings::{GFP_KERNEL, IS_ERR, PTR_ERR},
    error::to_result,
    new_mutex,
    prelude::*,
    str::CStr,
    sync::Mutex,
};

use super::SgTable;

/// A Symmetric Key Cipher wrapper.
///
/// The `crypto_skcipher` pointer is constructed by the `new` initializer.
/// The recommended way to create such instances is with the [`pin_init`].
///
/// # Examples
///
/// The following is examples of creating [`Cipher`] instances.
///
/// ```rust
/// use kernel::{c_str, stack_try_pin_init};
/// # use core::pin::Pin;
/// # use crate::crypto::skcipher::Cipher;
///
/// // Allocates an instance on stack.
/// stack_try_pin_init!(let foo = Cipher::new(c_str!("cbc(aes)"), 0, 0));
/// let foo: Result<Pin<&mut Cipher>> = foo;
///
/// // Allocate an instance by Box::pin_init.
/// let bar: Result<Pin<Box<Cipher>>> = Box::pin_init(Cipher::new(c_str!("cbc(aes)"), 0, 0));
/// ```
#[pin_data(PinnedDrop)]
pub struct Cipher {
    #[pin]
    lock: Mutex<()>,
    skcipher: NonNull<crate::crypto_skcipher>,
}

// SAFETY: `Cipher` is synchronized `Mutex`, so it's safe to be used on multiple
// threads concurrently.
unsafe impl Sync for Cipher {}

impl Cipher {
    /// Constructs a new initializer.
    ///
    /// Try to allocate an skcipher handler, `alg_name` specifies the driver
    /// name of the cipher, `type_` specifies the type of the cipher, `mask`
    /// specifies the mask for the cipher.
    ///
    /// Returns a type impl [`PinInit<Cipher>`] in case of success, or an error
    /// code in [`Error`].
    pub fn new(alg_name: &'static CStr, type_: u32, mask: u32) -> impl PinInit<Self, Error> {
        try_pin_init!(Self {
            lock <- new_mutex!(()),
            // SAFETY: `alg_name` has static lifetimes and live indefinitely.
            // Any error will be checked by `IS_ERR`.
            skcipher: unsafe {
                let skcipher = crate::crypto_alloc_skcipher(alg_name.as_char_ptr(), type_, mask);
                let const_ptr: *const core::ffi::c_void = skcipher.cast();
                if IS_ERR(const_ptr) {
                    let err = PTR_ERR(const_ptr) as i32;
                    return Err(to_result(err).err().unwrap());
                }
                NonNull::new_unchecked(skcipher)
            },
        })
    }

    /// Allocates a cipher request, which could be used to handle the cipher
    /// operations later.
    pub fn alloc_request(&self) -> Result<Request> {
        // SAFETY: `self.skcipher` is a valid cipher.
        let req = unsafe { crate::skcipher_request_alloc(self.skcipher.as_ptr(), GFP_KERNEL) };
        if req.is_null() {
            return Err(ENOMEM);
        }
        Ok(Request {
            // SAFETY: `req` is checked above, it is valid.
            req: unsafe { NonNull::new_unchecked(req) },
        })
    }

    /// Returns the size (in bytes) of the IV for the skcipher referenced by the
    /// cipher handle. This IV size may be zero if the cipher does not need an IV.
    pub fn ivsize(&self) -> usize {
        // SAFETY: `self.skcipher` is constructed by `new`, it is non-null and valid.
        unsafe {
            let crypto_alg = (*self.skcipher.as_ptr()).base.__crt_alg;
            let skcipher_alg = crate::container_of!(crypto_alg, crate::skcipher_alg, base);
            (*skcipher_alg).ivsize as _
        }
    }

    /// Returns the size (in bytes) of the request data structure. The `skcipher_request`
    /// data structure contains all pointers to data required for the skcipher operation.
    pub fn reqsize(&self) -> usize {
        // SAFETY: `self.skcipher` is constructed by `new`, it is non-null and valid.
        unsafe { (*self.skcipher.as_ptr()).reqsize as _ }
    }

    /// Returns the block size (in bytes) for the skcipher referenced with the cipher
    /// handle. The caller may use that information to allocate appropriate memory
    /// for the data returned by the encryption or decryption operation.
    pub fn blocksize(&self) -> usize {
        // SAFETY: `self.skcipher` is constructed by `new`, it is non-null and valid.
        unsafe {
            let crypto_alg = (*self.skcipher.as_ptr()).base.__crt_alg;
            (*crypto_alg).cra_blocksize as _
        }
    }

    /// Sets key for skcipher.
    ///
    /// The key length determines the cipher type. Many block ciphers implement
    /// different cipher modes depending on the key size, such as AES-128 vs AES-192
    /// vs. AES-256. When providing a 16 byte key for an AES cipher handle, AES-128
    /// is performed.
    ///
    /// Returns `Ok(())` if the setting of the key was successful, or an error
    /// code in [`Error`].
    pub fn set_key(&self, key: &[u8]) -> Result {
        // SAFETY: `self.skcipher` is constructed by `new`, it is non-null and valid.
        unsafe {
            to_result(crate::crypto_skcipher_setkey(
                self.skcipher.as_ptr(),
                key.as_ptr(),
                key.len() as u32,
            ))
        }
    }
}

#[pinned_drop]
impl PinnedDrop for Cipher {
    fn drop(self: Pin<&mut Self>) {
        let skcipher = self.skcipher.as_ptr();
        // SAFETY: `self.skcipher` is constructed by `new`, it is non-null and valid.
        unsafe {
            let base = addr_of_mut!((*skcipher).base);
            crate::crypto_destroy_tfm(skcipher as _, base);
        }
    }
}

/// A cipher request.
///
/// This is constructed by the `Cipher`, and is used to construct a
/// cipher operation then handle it.
pub struct Request {
    req: NonNull<crate::skcipher_request>,
}

impl Request {
    /// The kernel crypto API use `scatterlist` to transfer the memory buffer.
    const SCATTERLIST_SIZE: usize = 1;

    /// Set information needed to perform the cipher operation.
    ///
    /// The `src` and `dst` hold the associated data concatenated with the
    /// plaintext or ciphertext. The `cryptlen` specifies the number of bytes
    /// to process from `src`, `iv` specifies the IV for the cipher operation
    /// which must comply with the IV size returned by `Cipher::ivsize`.
    fn set_crypt(&self, src: &SgTable, dst: &SgTable, cryptlen: usize, iv: &[u8]) {
        // SAFETY: `self.req` is non-null and valid.
        unsafe {
            let req_src = addr_of_mut!((*self.req.as_ptr()).src);
            *req_src = (*src.0.get()).sgl;
            let req_dst = addr_of_mut!((*self.req.as_ptr()).dst);
            *req_dst = (*dst.0.get()).sgl;
            let req_cryptlen = addr_of_mut!((*self.req.as_ptr()).cryptlen);
            *req_cryptlen = cryptlen as _;
            let req_iv = addr_of_mut!((*self.req.as_ptr()).iv);
            *req_iv = iv.as_ptr() as _;
        }
    }

    /// Handles encryption request.
    ///
    /// Returns `Ok(())` if the cipher operation was successful, or an error
    /// code in [`Error`].
    pub fn encrypt(self, input: &[u8], iv: &[u8], output: &mut [u8]) -> Result {
        let mut src = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        let mut dst = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        // SAFETY: the index does not exceed `SCATTERLIST_SIZE`.
        unsafe {
            src.set_buf(0, input);
            dst.set_buf(0, output);
        }
        self.set_crypt(&src, &dst, input.len(), iv);
        // SAFETY: `self.req` is non-null and valid.
        unsafe { to_result(crate::crypto_skcipher_encrypt(self.req.as_ptr())) }
    }

    /// Handles decryption request.
    ///
    /// Returns `Ok(())` if the cipher operation was successful, or an error
    /// code in [`Error`].
    pub fn decrypt(self, input: &[u8], iv: &[u8], output: &mut [u8]) -> Result {
        let mut src = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        let mut dst = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        // SAFETY: the index does not exceed `SCATTERLIST_SIZE`.
        unsafe {
            src.set_buf(0, input);
            dst.set_buf(0, output);
        }
        self.set_crypt(&src, &dst, input.len(), iv);
        // SAFETY: `self.req` is non-null and valid.
        unsafe { to_result(crate::crypto_skcipher_decrypt(self.req.as_ptr())) }
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        // SAFETY: `self.req` is non-null and valid.
        unsafe { crate::kfree_sensitive(self.req.as_ptr() as _) };
    }
}