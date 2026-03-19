# Validation Rules Specification

## 1. Node ID Format
- Pattern: `^[a-z][a-z0-9]*\\.[a-z][a-z0-9]*$`
- Examples: "config.loader", "hw.rapl", "exec.harness"
- No spaces, no uppercase, no special chars

## 2. Layer Requirements
- Every node MUST have a `layer` field
- Layers must be one of: config, hardware, execution, database
- Inline nodes must explicitly declare layer
- Component nodes inherit layer from definition

## 3. Node Existence
- All referenced nodes must exist in components or be defined inline
- Edge sources and targets must be valid nodes
- Boundary node sets must contain valid nodes

## 4. Wildcard Expansion
- Wildcards expanded before validation
- Empty wildcard results trigger warning

## 5. Duplicate Detection
- No duplicate node IDs across entire diagram
- No duplicate edge definitions

## 6. Failure Modes
- `strict: true` → exit on first error
- `strict: false` → warn and continue
