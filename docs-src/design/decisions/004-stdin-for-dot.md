# ADR 004: Use Stdin for Dot Rendering

## Context
Original design used temporary .dot files that were written to disk then deleted.

## Decision
Pipe DOT strings directly to `dot` command via stdin.

## Consequences
+ No temporary files
+ No cleanup code needed
+ Faster (no disk I/O)
+ More reliable (no orphaned temp files)
- Requires subprocess pipe support (which we have)

## Status
Accepted - Improves original design.
