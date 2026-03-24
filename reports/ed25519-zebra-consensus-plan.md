# `ed25519-zebra` Batch Verification Plan For Consensus

Date: 2026-03-24

## Scope

- Use `ed25519-zebra` only for incoming Simplex consensus signature verification.
- First target: certificate verification in `validator/consensus/simplex/certificate.cpp`.
- Keep on-chain validation, block signature sets, wallets, ADNL, and generic `keys/encryptor.cpp` on the current OpenSSL path.
- Keep the current benchmark target `test/consensus/benchmark-simplex-cert.cpp` as the baseline harness. We will extend it later instead of replacing it now.

## Why Start With Certificates

- `Certificate<T>::from_tl(...)` already has the full signature set in one place, so it is the cleanest consensus-only integration point.
- All signatures inside one certificate verify the same `vote` payload, so we can build the final `consensus.dataToSign` bytes once per certificate instead of once per signature.
- This keeps threshold logic, duplicate detection, and validator-weight accounting in C++.
- Single-vote validation in `validator/consensus/simplex/votes.cpp` can stay on OpenSSL initially. That keeps the first change small.

## Main Constraints

### 1. Signature semantics are not drop-in

- `ed25519-zebra` `4.1.0` implements ZIP215 validation rules, and its docs explicitly call consensus-critical use out as sensitive.
- The current C++ path uses OpenSSL Ed25519 verification.
- Before any production use, we need an incompatibility corpus and a shadow-mode comparison to determine whether OpenSSL and ZIP215 accept exactly the same inputs on TON consensus messages.
- Until that is proven, `ed25519-zebra` should be treated as benchmark-only or shadow-mode only.

### 2. Batch verification is randomized

- `batch::Verifier::verify(...)` uses random 128-bit coefficients internally.
- That is acceptable for performance testing, but for consensus it needs an explicit decision:
  - either accept the randomized soundness model,
  - or replace it with a reviewed deterministic construction,
  - or keep a second deterministic acceptance gate.
- This is the main blocker for enabling batch verification on the live consensus path.

### 3. Batch failure does not identify the bad signer by itself

- `Certificate<T>::from_tl(...)` currently reports the invalid validator precisely.
- `ed25519-zebra` batch verification only tells us whether the whole batch passed.
- On batch failure, the Rust side should fall back to per-item verification and return a validity bitmap so C++ can preserve the current error reporting.

## Recommended Architecture

### C++ side

- Add a consensus-local wrapper, for example:
  - `validator/consensus/ed25519_batch_verifier.h`
  - `validator/consensus/ed25519_batch_verifier.cpp`
- Keep all parsing, duplicate checks, validator-index checks, and weight checks in C++.
- In `validator/consensus/simplex/certificate.cpp`:
  - parse the certificate as today,
  - collect `(validator index, public key bytes, signature bytes)` items,
  - build `consensus.dataToSign(session_id, vote_bytes)` once,
  - call the batch wrapper only if every key is Ed25519,
  - fall back to the existing OpenSSL path for unexpected key types or disabled feature flags.
- Do not route this through `PeerValidator::check_signature(...)` for the batch path. That function is intentionally generic and is still needed for single-message verification in other consensus paths.

### Rust side

- Add a new small crate inside the existing Rust workspace, for example:
  - `rust_implementation/ton-rust-node/src/node/consensus-ed25519-batch`
- Recommended crate type for the integration target: `staticlib`.
  - This keeps the feature internal to the node binary and avoids deploying a separate shared library.
  - Use the existing Rust `extern "C"` style from the in-tree `emulator` crate as the FFI pattern reference.
- Proposed exported API:

```c
typedef struct {
  const uint8_t *msg_ptr;
  size_t msg_len;
  uint8_t public_key[32];
  uint8_t signature[64];
} TonConsensusEd25519Item;

typedef struct {
  uint32_t status;
  uint32_t valid_count;
  uint8_t *validity_bitmap;
} TonConsensusEd25519Result;

uint32_t ton_consensus_ed25519_batch_verify(
    const TonConsensusEd25519Item *items,
    size_t item_count,
    TonConsensusEd25519Result *result);
```

- Rust implementation details:
  - convert each item into `VerificationKeyBytes`, `Signature`, and message slice,
  - queue them into `ed25519_zebra::batch::Verifier`,
  - if batch verification succeeds, mark all items valid,
  - if batch verification fails, run per-item `verify_single()` and fill the bitmap.

## Integration Phases

### Phase 0: crypto decision

- Decide whether ZIP215 semantics are acceptable for TON consensus.
- Decide whether randomized batch verification is acceptable for consensus.
- If either answer is "not yet", limit the work to benchmark-only or shadow-mode.

### Phase 1: FFI prototype

- Add the Rust crate with a minimal C ABI.
- Build it only behind a dedicated CMake option such as `TON_ENABLE_ZEBRA_CONSENSUS_BATCH`.
- Add unit tests on the Rust side for:
  - valid single item,
  - valid batch,
  - one bad signature in batch,
  - malformed public key,
  - malformed signature.

### Phase 2: benchmark-only integration

- Add the C++ wrapper and call it only from a benchmark code path first.
- Keep production consensus behavior on OpenSSL.
- Extend the existing benchmark later with selectable backends:
  - `openssl-single`
  - `zebra-batch-cert`
  - `zebra-batch-window`
- Measure both certificate-local batching and cross-certificate batching windows.

### Phase 3: certificate path shadow-mode

- Integrate the wrapper into `validator/consensus/simplex/certificate.cpp` behind a runtime gate.
- In shadow-mode:
  - run zebra batch verification,
  - also run the current OpenSSL path,
  - log any mismatch with validator index, slot, vote type, and hex inputs.
- Do not change accept/reject decisions in this phase.

### Phase 4: live certificate verification

- Switch certificate verification to the zebra path only after:
  - zero mismatches on devnet and replay corpus,
  - clear decision on ZIP215 semantics,
  - clear decision on randomized batch verification.
- Keep OpenSSL single-item fallback on batch failure until operational confidence is high.

### Phase 5: optional cross-certificate batching

- If certificate-local batching is not enough, add a small consensus-local queue that batches multiple verification jobs together.
- This is where `ed25519-zebra` can benefit from repeated validator keys across consecutive certificates.
- Suggested first window:
  - up to `8` certificates,
  - or up to `250-500us` wait time,
  - whichever comes first.
- This step should be optional because it adds latency and queueing complexity.

## Benchmark Plan For Later

- Keep `test/consensus/benchmark-simplex-cert.cpp` as-is for now.
- Later extend it instead of replacing it:
  - add a backend selector,
  - add a batching-window selector,
  - add a mode that feeds consecutive certificates through one batch,
  - add a mode with injected invalid signatures to measure fallback cost.
- Report at least:
  - total verification time,
  - average certificate verification time,
  - average signature verification time,
  - batch success rate,
  - fallback rate,
  - extra queueing latency for windowed batching.

## Concrete Code Touch Points

- Current certificate verification loop:
  - `validator/consensus/simplex/certificate.cpp`
- Current single-vote verification path:
  - `validator/consensus/simplex/votes.cpp`
- Current generic signature path:
  - `validator/consensus/types.cpp`
- Raw Ed25519 public key access for batch items:
  - `keys/keys.hpp`

## Recommendation

- Recommended first implementation target: certificate-local batch verification behind a compile-time flag and shadow-mode runtime gate.
- Recommended first non-code task: resolve the ZIP215 compatibility question and the randomized-batch policy question.
- Recommended first benchmark extension after that: compare `openssl-single` against `zebra-batch-cert` before attempting cross-certificate queueing.

## External References

- `ed25519-zebra` crate docs: https://docs.rs/ed25519-zebra/latest/ed25519_zebra/
- `ed25519-zebra` batch verifier docs and source: https://docs.rs/ed25519-zebra/latest/src/ed25519_zebra/batch.rs.html
