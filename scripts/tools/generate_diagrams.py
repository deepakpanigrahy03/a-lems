#!/usr/bin/env python3
"""
A-LEMS Diagram Generator
--------------------------------
Main entry point for generating diagrams from YAML definitions.

This script:
1. Loads all diagram configurations from config/diagrams/
2. Validates them for errors
3. Resolves components and wildcards
4. Builds DOT format graphs
5. Renders SVG files to docs/assets/diagrams/

Usage:
    python generate_diagrams.py              # Generate all diagrams
    python generate_diagrams.py --name architecture  # Generate specific diagram
    python generate_diagrams.py --help        # Show help
"""

import argparse
import sys
from pathlib import Path

# Add tools directory to path
sys.path.append(str(Path(__file__).parent))

# Import our diagram processor classes
from diagram_processor import (
    DiagramLoader,
    DiagramValidator,
    ComponentResolver,
    DotBuilder,
    SvgRenderer
)

# Import path configuration
from path_loader import config


def get_project_root() -> Path:
    """Find the project root directory (where .git is)."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / '.git').exists():
            return current
        current = current.parent
    return Path.cwd()


def load_and_validate_diagrams(config_dir: Path):
    """
    Load all diagram configurations and validate them.
    
    This function:
    1. Loads all YAML files
    2. Validates each diagram instance
    3. Returns loaded data if valid, exits if errors found
    """
    print("📂 Loading diagram configurations...")
    
    # Create loader and load all files
    loader = DiagramLoader(config_dir)
    data = loader.load_all()
    
    # Create validator with rules from YAML
    validator = DiagramValidator(data['validation'])
    
    # Validate each diagram instance
    print("🔍 Validating diagrams...")
    all_valid = True
    
    for instance in data['instances']:
        instance_name = instance.get('name', 'unknown')
        print(f"   Checking: {instance_name}")
        
        valid = validator.validate_instance(instance, data['components'])
        if not valid:
            all_valid = False
    
    # Show validation report
    report = validator.get_report()
    if report['errors']:
        print("\n❌ Validation errors found:")
        for error in report['errors']:
            print(f"   • {error}")
    
    if report['warnings']:
        print("\n⚠️  Warnings:")
        for warning in report['warnings']:
            print(f"   • {warning}")
    
    if not all_valid:
        print("\n❌ Validation failed. Please fix errors and try again.")
        sys.exit(1)
    
    print("✅ All diagrams valid!")
    return data


def generate_diagram(instance: dict, data: dict, output_dir: Path):
    """
    Generate a single diagram from its instance definition.
    
    This function:
    1. Gets all node IDs from the instance
    2. Resolves components and wildcards
    3. Builds DOT format
    4. Renders SVG
    """
    instance_name = instance.get('name', 'unknown')
    template_name = instance.get('template', 'layered')
    
    print(f"\n📊 Generating: {instance_name}")
    
    # ================================================================
    # Step 1: Get all node IDs from this instance
    # ================================================================
    all_node_ids = []
    for node_def in instance.get('nodes', []):
        if isinstance(node_def, str):
            all_node_ids.append(node_def)
        elif isinstance(node_def, dict):
            all_node_ids.append(list(node_def.keys())[0])
    
    # ================================================================
    # Step 2: Create resolver and resolve all nodes
    # ================================================================
    resolver = ComponentResolver(
        data['components'],
        data['templates'],
        data['boundaries']
    )
    
    resolved_nodes = []
    for node_def in instance.get('nodes', []):
        resolved = resolver.resolve_node(node_def, all_node_ids)
        resolved_nodes.extend(resolved)
    
    # ================================================================
    # Step 3: Build DOT graph
    # ================================================================
    builder = DotBuilder(template_name, data['templates'])
    
    # Start building DOT string
    dot_lines = [builder.build_graph_header()]
    
    # Add all nodes
    for node in resolved_nodes:
        dot_lines.append(builder.build_node(node))
    
    # Add all edges
    edge_styles = data['templates'][template_name].get('edge_styles', {})
    for edge in instance.get('edges', []):
        dot_lines.append(builder.build_edge(edge, edge_styles))
    
    # Close graph
    dot_lines.append("}")
    
    # Combine all lines
    dot_string = "\n".join(dot_lines)






    # ================================================================
    # Step 4: Render to SVG
    # ================================================================
    output_file = output_dir / f"{instance_name}.svg"
    # DEBUG: Print first 20 lines of DOT
    print("\n--- DEBUG DOT (first 20 lines) ---")
    for i, line in enumerate(dot_string.split('\n')[:20]):
        print(f"{i+1}: {line}")
    print("-----------------------------------\n")

    # ================================================================
    # Step 4: Save DOT for debugging
    # ================================================================
    debug_dot = output_dir / f"{instance_name}.debug.dot"
    with open(debug_dot, 'w') as f:
        f.write(dot_string)
    print(f"📝 Saved debug DOT to: {debug_dot}")    
    renderer = SvgRenderer()
    success = renderer.render(dot_string, output_file)

    renderer = SvgRenderer()
    
    success = renderer.render(dot_string, output_file)
    
    if success:
        print(f"   ✅ Saved: {output_file}")
    else:
        print(f"   ❌ Failed: {instance_name}")
    
    return success


def main():
    """Main entry point."""
    # ================================================================
    # Set up command line arguments
    # ================================================================
    parser = argparse.ArgumentParser(
        description="Generate diagrams from YAML definitions"
    )
    parser.add_argument(
        "--name",
        help="Generate only specific diagram (by name)"
    )
    parser.add_argument(
        "--output",
        help="Output directory (default: from paths.yaml)"
    )
    
    args = parser.parse_args()
    
    # ================================================================
    # Find configuration and output directories
    # ================================================================
    project_root = get_project_root()
    config_dir = project_root / "config" / "diagrams"
    
    # Use output from paths.yaml, or override if provided
    if args.output:
        output_dir = Path(args.output)
    else:
        output_dir = config.DIAGRAMS_OUTPUT
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("🔧 A-LEMS Diagram Generator")
    print("=" * 60)
    print(f"Config:  {config_dir}")
    print(f"Output:  {output_dir}")
    print()
    
    # ================================================================
    # Load and validate all diagrams
    # ================================================================
    data = load_and_validate_diagrams(config_dir)
    
    # ================================================================
    # Generate diagrams
    # ================================================================
    print("\n🎨 Generating diagrams...")
    
    generated = 0
    failed = 0
    
    for instance in data['instances']:
        instance_name = instance.get('name', 'unknown')
        
        # Skip if specific diagram requested and this isn't it
        if args.name and instance_name != args.name:
            continue
        
        success = generate_diagram(instance, data, output_dir)
        if success:
            generated += 1
        else:
            failed += 1
    
    # ================================================================
    # Show summary
    # ================================================================
    print("\n" + "=" * 60)
    print("📊 Generation Complete")
    print("=" * 60)
    print(f"✅ Generated: {generated}")
    if failed > 0:
        print(f"❌ Failed:    {failed}")
    print(f"📁 Output:    {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()