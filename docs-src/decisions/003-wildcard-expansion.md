# ADR 003: Wildcard Expansion for Node Groups

## Context
Many diagrams need to reference entire layers or groups (e.g., "all hardware components").

## Decision
Support `*` wildcard in node lists:
- `config.*` → all config layer nodes
- `hw.*` → all hardware nodes
- `exec.*` → all execution nodes

Implementation: Expand before validation.

## Consequences
- + Concise diagram definitions
- + Self-documenting (reader knows "hw.*" means all hardware)
- - Requires expansion logic in generator
- - Must handle empty matches gracefully
