# Fast-startup WAL merge

For [#17058](https://github.com/prometheus/prometheus/issues/17058) and
[PR #18542](https://github.com/prometheus/prometheus/pull/18542).

## Status

The branch implements an experimental durable generation protocol, with
regression tests for repeated restarts, checkpoints, overlap, interrupted replay,
and repair. The user explicitly approved a WAL format extension. This document
does not certify upstream merge readiness or production-scale performance; the
remaining acceptance work is listed below.

## Startup and cutover

- Establish a series-ID high watermark from the saved state, checkpoint and WAL
  tail before ingestion starts. Allocation order and commit order can differ.
  The state file is a hint even after a clean shutdown, since a later run may
  have written with fast startup disabled. Invalid hints trigger a full scan;
  scan failures fall back to synchronous initialization.
- Sync state files before atomic rename and directory sync. Write a clean marker
  only after successful replay and WAL closure.
- Finish snapshot/mmap recovery before admitting ingestion: recovery can replace
  the maps or remove corrupted chunk files. Existing WBL, snapshotting, exemplar
  storage or a disabled WAL select synchronous initialization. Configured OOO
  remains incompatible with fast startup.
- Replay the historical prefix into a shadow map; ingestion writes to a fresh
  segment. Shadow creation does not publish postings or live-series counts.
- After replay, stitch one series at a time under its lock while ingestion
  continues. Queued samples and metadata reserve their series until Commit or
  Rollback; a series with a reservation is retried after its transaction ends, so
  a Commit never applies samples validated against live data alone to the merged
  history. Shutdown can cancel the retries.
- Keep the live object/ref during stitching. Cached references remain valid.
  Build postings unordered and sort once, avoiding quadratic insertion costs.
- Reads, readiness, rules, compaction, retention and WAL truncation require
  successful replay. `WaitForWALReplay` signals completion, not success;
  callers check `WALReplayError`. Closed live chunks can still be mmapped during
  replay under the usual series locks, bounding ingestion heap usage.
- Cancel replay and stop its workers before closing the mapper/WAL. Failed DB
  initialization releases resources. The directory stays locked until Head/WAL
  closure completes. Rules waiting for readiness can stop; alert restoration
  retries after a temporary querier error.

## Durable identities

Ordinary Series records with duplicate labels indicate recreation after
compaction and replace earlier data. Concurrent startup creates duplicate labels
without compaction. Keeping all ordinary duplicates would break existing replay
semantics, including `TestHead_WALMultiRef`.

ConcurrentSeries (type 12, ordinary Series payload) distinguishes the new case.
The attribute belongs to the series at creation, not commit time. Workers keep
each source separate through all WAL segments, because records from different
sources can interleave. A repeated concurrent definition is idempotent. An
ordinary Series definition still supersedes preceding sources after compaction.
Series created after a failed replay, before ingestion stops, still use concurrent
definitions: failure is not evidence that preceding history has been compacted.

The original reproduction lost 400 of 402 samples on the second restart. It now
retains all samples. Tests also exercise repeated fast and ordinary restarts,
interleaved sources, checkpointing, recreation and continuation writes.

Checkpoints preserve concurrent definitions and retain source refs until their
samples expire. Metadata updates preserve last-update order across refs. Metadata
is persisted under the surviving series reference before an alias can expire,
including metadata-only sources. A repaired prefix persists this correction after
repair, which would otherwise discard it. OOO mmap chunks and WBL aliases remain
attached during subsequent feature-off replay.

Older versions cannot safely recover this WAL. Turning the flag off does not
restore downgrade compatibility. Migrate through a block snapshot/export to a
separate directory without the experimental WAL before downgrading.

## Reconciliation

Non-overlapping chunks are linked without decoding. Historical memory chunks
retain their source ref when eventually mmapped. Chunk GC checks all retained
file refs: timestamp order no longer implies file order.

Overlapping sources are merged in timestamp order. Distinct timestamps survive;
the newer source wins equal timestamps, including sample-type conflicts. The
merge handles floats, integer/float histograms, and float start timestamps.
Counter-reset hints are re-evaluated at source changes. Disjoint historical mmap
prefixes are reused; overlapping tails are recoded. Transaction rings preserve
the association between samples and append IDs. Empty live series adopt history's
tail and duplicate-detection state.

Tombstones apply to the sources present when the delete was recorded, not future
independently ingested data. Reconciliation applies source-local intervals before
combining samples. Full deletion drains workers before removing identities.
Metadata falls back to history only when live ingestion has no replacement.
Stale-series and chunk accounting are adjusted for removed/replaced data.

Mixed-source chunks use disk-series ref zero and are not used as replay caches.
Otherwise the maximum cached timestamp could hide holes in a source's WAL
stream. Single-source chunks remain reusable; WAL records are authoritative
until compaction. Chunk snapshots are skipped when concurrent or aliased records
are replayed, and old snapshots are removed before concurrent ingestion. This also
protects later feature-off runs and ordinary series recreation after compaction:
the snapshot format does not capture source aliases. TSDB block snapshots are
unaffected.

When sources do not overlap, every chunk keeps a single source. Replay folds
older sources onto the newest one, which survives: it owns its head chunks and
the latest data, so later appends extend chunks it owns and the next replay folds
older sources onto it again without re-encoding. A live series with no data that
adopts history first writes history's head chunks under their own ref.

Only series whose sources overlap are re-encoded. Their new chunks remain uncached
for that in-memory series's lifetime, and a subsequent replay can restore normal
caching once obsolete source aliases have expired.

The stitch does not pause ingestion. Appends to a series briefly wait for its
lock while that series is stitched, which takes longer when its sources overlap.

## Corruption and interrupted startup

ReplayBoundary (type 13, no payload) is synced as the first record of the live
segment before ingestion starts. When background replay finds corruption in the
history it is replaying, it discards that history from the corruption up to its
boundary, as ordinary repair discards everything after a corruption, and serves
the valid prefix together with the live suffix. Segments are truncated rather
than deleted to keep indices sequential. Ordinary startup still refuses to
discard a suffix beyond a later boundary; files remain available for manual
recovery. Tests cover malformed sample payloads with valid checksums, which pass
the ID scan but fail during background decoding, and checksum mismatches in
segments the ID scan skips. Ordinary repair of a corrupt final tail remains supported:
its valid prefix is reconciled before serving it. Interrupted replay can recover
the historical prefix and acknowledged live suffix on the next startup. A
subprocess test kills the writer during replay and after reconciliation, without
shutdown cleanup; two subsequent ordinary restarts verify every committed sample,
including overlapping data and mmapped chunks. This tests process death, not
power-loss durability.

## Validation and performance

```sh
go test ./... -count=1 -timeout=10m
go test -race ./tsdb ./rules ./tsdb/wlog -run 'Test(Head.*FastStartup|HeadConcurrentWAL|HeadWALRecreatedSeriesSnapshot|HeadStaleCountAcrossSampleTypes|MemSeriesPrependHistory|ManagerStopBeforeRun|Group_RetryStateRestoration|RepairPreservesConcurrentReplaySuffix)' -count=3 -timeout=180s
go test ./tsdb -run '^$' -bench '^(BenchmarkHeadStartup|BenchmarkHeadFastStartupMerge)$' -benchmem -benchtime=1x -count=6
make lint
git diff --check
```

The postings/merge microbenchmark originally took approximately 70 ms, 274 ms and
1,074 ms for 10k, 20k and 40k WAL-only series. Deferring sorting brings these to
approximately 6 ms, 14 ms and 28 ms. The timed allocation increase of approximately
60 KB includes sorting that the original baseline timed separately.

The end-to-end benchmark uses 240 float samples per series at 15-second spacing,
Snappy WAL, four replay workers, no chunk cache or allocator hint, and an initial
scrape touching every series. WAL fixture creation and `NewHead` construction are
outside the timed region. Six runs on an Intel Xeon 6975P-C gave these medians:

| Series / historical samples | Ordinary first scrape and readiness | Fast first scrape | Fast readiness | Allocated bytes, ordinary / fast |
| --- | ---: | ---: | ---: | ---: |
| 10,000 / 2.4 million | 81.78 ms | 43.00 ms | 100.07 ms | 70.78 / 77.79 MiB |
| 100,000 / 24 million | 790.2 ms | 477.0 ms | 1,000.9 ms | 449.7 / 544.1 MiB |
| 1,000,000 / 240 million | 12,947 ms | 5,400 ms | 14,957 ms | 4,824.1 / 6,130.8 MiB |

First-scrape latency improves 47%, 40% and 58%; readiness is 22%, 27% and 16%
slower. The extra scan, dual maps and reconciliation account for the tradeoff.
At 100k and one million series, allocated bytes increase 21% and 27%.
These are allocation totals, not peak RSS.
Feature-off ingestion was compared against branch HEAD using
`BenchmarkHeadAppender_AppendCommit` (100 series, five float samples per append,
one-second runs, six runs of each appender). Alternating baseline and current
binaries gave v1 medians of 118.9 / 119.2 microseconds (not statistically
significant), and v2 medians of 118.9 / 119.8 microseconds (+0.78%, p=0.002).
Both retained 49 allocations per operation, with no significant byte-allocation
change. The small v2 cost remains a performance tradeoff to review, not a claimed
improvement. Initial non-alternating measurements overstated it at 1–2%.
The baseline at `c8ae9bdf8` required an unused-variable compile fix and a nil
shadow-map shutdown guard; neither changes the timed feature-off append path.
The comparison is against the original implementation on this branch, not
upstream main.

## Remaining upstream acceptance work

- Agree on experimental record IDs, the downgrade restriction, manual-recovery
  policy, snapshot fallback and equal-timestamp conflict semantics.
- Measure peak RSS, disk/CPU consumption and p99/p999 ingestion latency during
  cutover under sustained representative load, including histogram-heavy overlap,
  slow disk and million-series cardinality. The float benchmark is not a soak test.
- Validate power-loss recovery on supported filesystems; process-kill and
  deterministic cancellation/corruption tests do not simulate every fsync failure.
- Split the preparatory lifecycle fixes, WAL format/replay changes and feature
  integration into reviewable, independently tested commits with DCO sign-offs.

Suggested PR release note (after acceptance):

```release-notes
[FEATURE] TSDB: Add experimental ingestion during WAL replay with durable concurrent series generations. This feature writes WAL records incompatible with older Prometheus versions.
```
