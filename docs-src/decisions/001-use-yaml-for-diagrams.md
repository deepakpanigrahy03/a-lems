# ADR 001: Use YAML for Diagram Configuration

## Context
We need a declarative way to define system diagrams that can be version-controlled and auto-generated.

## Decision
Use YAML for all diagram configuration files because:
- Human-readable
- Git-friendly
- Easy to parse in Python
- Supports comments
- Hierarchical structure matches our needs

## Consequences
- + Simple to edit
- + Easy to validate
- + Can be generated from other tools
- - Need custom parser for our specific schema
- - No built-in validation (must implement ourselves)
