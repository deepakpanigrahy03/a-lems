# ADR 002: Namespaced Node IDs

## Context
With many components across layers, we need to avoid ID collisions and make relationships clear.

## Decision
Use format `namespace.name` for all node IDs:
- `config.loader`
- `hw.rapl`
- `exec.harness`
- `db.main`

## Consequences
- + No ID collisions across layers
- + Clear which layer a component belongs to
- + Wildcard matching becomes simple (`config.*`)
- - Slightly longer IDs
- - Must enforce format in validation
