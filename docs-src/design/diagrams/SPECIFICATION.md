# Diagram System Specification

## Overview
Config-driven diagram generation system with layered architecture.

## Core Components
- **Components**: Reusable building blocks (config.diagrams.components.yaml)
- **Templates**: Rendering rules (config.diagrams.templates.yaml)
- **Boundaries**: System boundaries (config.diagrams.boundaries.yaml)
- **Instances**: Actual diagrams (config.diagrams.instances/*.yaml)

## Processing Pipeline
1. Load all YAML files
2. Validate against rules
3. Resolve components and wildcards
4. Build DOT graph
5. Render to SVG
## Renderer (SvgRenderer)

## Renderer (SvgRenderer)

**Original Design:**
- Write DOT to temporary file
- Run `dot` command
- Delete temp file
- ❌ Problems: disk I/O, cleanup errors, orphaned files

**Improved Design (Implementation):**
- Pipe DOT string directly to `dot` via stdin
- ✅ No temporary files
- ✅ Faster (no disk I/O)
- ✅ No cleanup needed
- ✅ More reliable

**Implementation:**
```python
result = subprocess.run(
    ['dot', '-Tsvg', '-o', output_path],
    input=dot_string,  # Pipe via stdin
    capture_output=True,
    text=True
)
text

---

## ✅ **But Even Without That - Your Update is CORRECT**

The document now reflects the actual implementation. This is exactly how design docs should evolve.