# Fork-Mode Register Fetching

This note documents the register-fetching changes made to fork mode to improve
correctness for capability-heavy accounts and Atree-backed account storage.

## Background

In fork mode, the emulator serves local execution while lazily reading missing
ledger registers from a remote Access API node.

That works well for stable account metadata, but Cadence account storage and
capabilities are backed by dynamic Atree slab keys. Those keys are sparse and
are only known precisely when the FVM touches them.

## Problem

The previous implementation tried to reduce round-trips by prefetching guessed
slab keys for an account up front. In practice this caused two classes of
problems:

1. Batch reads are brittle for speculative keys.
   `GetRegisterValues` may fail the whole request if any requested register is
   missing, which is common when guessed Atree slab keys are sparse.

2. Missing and transient failures were treated as empty register values.
   Returning empty bytes for `NotFound`, timeouts, or invalid requests caused
   local caches to record an absence that was never actually confirmed.

The result was cache poisoning: a later exact lookup could incorrectly reuse
that empty value and surface misleading Cadence errors such as:

```text
slab (...) not found: map slab not found
```

## What Changed

### 1. Single-register reads are now tri-state

`getRegisterSingle` now returns:

- a register value
- a `found` flag
- an error

This separates three cases that were previously conflated:

- the register exists
- the register is definitively missing
- the remote fetch failed transiently

### 2. Missing registers are not cached

If the remote node returns `NotFound`, fork mode now treats that as "missing for
this lookup" and returns an empty value to the FVM without persisting anything
to the in-memory LRU or local SQLite store.

That keeps speculative misses from poisoning future exact reads.

### 3. Transient transport failures now propagate as errors

Errors such as `DeadlineExceeded` or `InvalidArgument` are no longer coerced to
empty bytes. They now bubble up so callers can retry instead of silently
recording a fake empty register.

### 4. Owner prefetch no longer guesses Atree slab keys

Owner prefetch now only warms a small set of stable internal account metadata
registers, such as:

- account status
- the first public key register
- contract names

It no longer tries to enumerate or batch-fetch all slab keys for an account.

### 5. Dynamic slabs are fetched lazily and exactly

Atree slab keys are now fetched on demand when the FVM resolves the exact
register ID it needs. Successful exact reads are still cached normally.

This is less aggressive than speculative slab prefetch, but it is correct for
sparse dynamic keys and produces stable behavior under repeated access.

## Why We Do Not Enumerate Slabs From `storage_index`

The previous implementation used `storage_index` as the basis for slab
enumeration. That assumption proved too fragile for real-world capability and
Atree access patterns.

Fork mode now avoids deriving a complete slab list from account metadata and
instead relies on exact lazy reads driven by the FVM.

## Tests Added

The change is covered by:

- unit tests that verify missing registers do not poison caches
- unit tests that verify transient remote errors do not poison caches
- a fork integration test that reads a public `FlowTokenReceiver` capability
  from a real mainnet account

## Practical Impact

This change trades some speculative prefetching for correctness.

In exchange, fork mode becomes much more reliable for:

- public capability borrowing
- Flow token vault access
- account storage paths backed by Atree slabs
- simulator workloads that read arbitrary mainnet accounts

If you want better warm-start behavior across restarts, run fork mode with
`--persist`. If you need reproducible results across multiple emulator
instances, also pin a shared `--fork-height`.
