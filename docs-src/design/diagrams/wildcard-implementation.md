# Wildcard Implementation Specification

## Purpose
Allow pattern matching for node groups (e.g., "config.*" matches all config nodes)

## Behavior
- Pattern ending with `*` matches all nodes with that prefix
- Pattern without `*` treated as literal node ID
- Returns empty list if no matches

## Algorithm
function match_wildcard(pattern, all_nodes):
if pattern ends with '*':
prefix = pattern without last character
return [node for node in all_nodes if node starts with prefix]
else:
if pattern in all_nodes:
return [pattern]
else:
return []

text

## Example
Input: pattern = "config.*", nodes = ["config.loader", "config.db", "hw.rapl"]
Output: ["config.loader", "config.db"]

## Validation Rules
- Wildcard patterns must end with `.*`
- No nested wildcards (e.g., "config.*.loader" not allowed)
- Must match at least one node (warning if empty)
