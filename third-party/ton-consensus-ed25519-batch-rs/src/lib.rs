use ed25519_zebra::{batch, Signature, VerificationKeyBytes};
use rand::thread_rng;
use std::{convert::TryInto, panic::catch_unwind, ptr::NonNull, slice};

const STATUS_ALL_VALID: u32 = 0;
const STATUS_HAS_INVALID: u32 = 1;
const STATUS_INVALID_ARGUMENT: u32 = 2;
const STATUS_PANIC: u32 = 3;

#[repr(C)]
pub struct TonConsensusEd25519Batch {
    pub message_ptr: *const u8,
    pub message_len: usize,
    pub public_keys_ptr: *const u8,
    pub signatures_ptr: *const u8,
    pub item_count: usize,
    pub validity_ptr: *mut u8,
}

fn make_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        Some(&[])
    } else if ptr.is_null() {
        None
    } else {
        Some(unsafe { slice::from_raw_parts(ptr, len) })
    }
}

fn make_slice_mut<'a>(ptr: *mut u8, len: usize) -> Option<&'a mut [u8]> {
    if len == 0 {
        Some(unsafe { slice::from_raw_parts_mut(NonNull::<u8>::dangling().as_ptr(), 0) })
    } else if ptr.is_null() {
        None
    } else {
        Some(unsafe { slice::from_raw_parts_mut(ptr, len) })
    }
}

fn verify_batch(batch_ptr: *const TonConsensusEd25519Batch) -> u32 {
    if batch_ptr.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let batch = unsafe { &*batch_ptr };
    if batch.item_count == 0 {
        return STATUS_ALL_VALID;
    }

    let Some(public_keys_len) = batch.item_count.checked_mul(32) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(signatures_len) = batch.item_count.checked_mul(64) else {
        return STATUS_INVALID_ARGUMENT;
    };

    let Some(message) = make_slice(batch.message_ptr, batch.message_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(public_keys) = make_slice(batch.public_keys_ptr, public_keys_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(signatures) = make_slice(batch.signatures_ptr, signatures_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(validity) = make_slice_mut(batch.validity_ptr, batch.item_count) else {
        return STATUS_INVALID_ARGUMENT;
    };

    validity.fill(0);

    let mut verifier = batch::Verifier::new();
    let mut items = Vec::with_capacity(batch.item_count);
    for idx in 0..batch.item_count {
        let public_key_offset = idx * 32;
        let signature_offset = idx * 64;

        let public_key: [u8; 32] = public_keys[public_key_offset..public_key_offset + 32]
            .try_into()
            .expect("public key slice length is fixed");
        let signature: [u8; 64] = signatures[signature_offset..signature_offset + 64]
            .try_into()
            .expect("signature slice length is fixed");

        let item = batch::Item::from((
            VerificationKeyBytes::from(public_key),
            Signature::from(signature),
            message,
        ));
        verifier.queue(item.clone());
        items.push(item);
    }

    if verifier.verify(thread_rng()).is_ok() {
        validity.fill(1);
        return STATUS_ALL_VALID;
    }

    let mut all_valid = true;
    for (validity_slot, item) in validity.iter_mut().zip(items.into_iter()) {
        let is_valid = item.verify_single().is_ok();
        *validity_slot = u8::from(is_valid);
        all_valid &= is_valid;
    }

    if all_valid {
        STATUS_ALL_VALID
    } else {
        STATUS_HAS_INVALID
    }
}

#[no_mangle]
pub extern "C" fn ton_consensus_ed25519_batch_verify(
    batch_ptr: *const TonConsensusEd25519Batch,
) -> u32 {
    catch_unwind(|| verify_batch(batch_ptr)).unwrap_or(STATUS_PANIC)
}
