# Compaction

This document covers compaction planning and scheduling in `src/storage/compaction`.

## Goal

Compaction reduces read/write/space amplification by merging SSTables according to a strategy.

Current implementation focus:
- strategy planning
- scheduling and prioritization
- amplification metrics

## Strategies

### Leveled compaction (`leveled.rs`)

Config:
- `level0_file_limit`
- `level_size_base_bytes`
- `level_size_multiplier`
- `max_levels`

Triggers:
- `Level0Overflow` when L0 file count exceeds threshold
- `LevelSizeExceeded` when level bytes exceed target

Plan output includes:
- source level / target level
- chosen source input(s)
- overlapping target inputs
- score

### Tiered compaction (`tiered.rs`)

Config:
- `max_components_per_tier`
- `min_tier_size_bytes`
- `output_level`

Behavior:
- groups tables by size bucket (tier)
- triggers when a tier has enough components
- outputs merge plan for selected tier with score

## Scheduler (`scheduler.rs`)

`CompactionScheduler` provides:
- enqueue/pop of compaction tasks
- priority ordering by plan score
- dedup by plan signature
- in-flight task tracking

It supports both strategy types through:
- `CompactionStrategy::Leveled`
- `CompactionStrategy::Tiered`

## Metrics

`CompactionMetrics` tracks:
- user bytes written
- compaction bytes written
- point lookup checks and requests
- live bytes / total bytes on disk
- completed compactions

Derived helpers:
- write amplification
- read amplification
- space amplification

## Current integration status

Compaction planning and scheduler infrastructure are implemented and benchmarked.

Remaining work:
- integrate actual compaction execution loop into `StorageEngine`
- materialize on-disk merge output and manifest edits as background runtime behavior
- connect scheduler metrics to live engine compaction lifecycle end-to-end
