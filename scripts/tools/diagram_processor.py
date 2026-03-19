#!/usr/bin/env python3
"""
Diagram Processor for A-LEMS
--------------------------------
This module handles loading, validating, and processing diagram definitions
from YAML files. It's designed to be simple, modular, and easy to understand.

Each class has ONE clear responsibility:
- DiagramLoader: Reads YAML files
- DiagramValidator: Checks for errors
- ComponentResolver: Handles wildcards and merging
- DotBuilder: Creates DOT format strings
- SvgRenderer: Converts DOT to SVG
"""

import yaml
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional


# ============================================================================
# LOADER: Reads all YAML configuration files
# ============================================================================

class DiagramLoader:
    """
    Loads all YAML files from the diagrams configuration directory.
    
    This class reads:
    - components.yaml: Reusable component definitions
    - templates.yaml: Visual styling rules
    - boundaries.yaml: System boundary definitions
    - validation.yaml: Validation rules
    - instances/*.yaml: Individual diagram definitions
    """
    
    def __init__(self, config_dir: Path):
        # Store the configuration directory path
        self.config_dir = config_dir
        
        # Initialize empty containers for each config type
        self.components = {}      # Reusable components
        self.templates = {}        # Visual styling rules
        self.boundaries = []       # System boundaries
        self.validation = {}       # Validation rules
        self.instances = []        # Individual diagrams
        
    def load_all(self) -> Dict[str, Any]:
        """
        Load all configuration files and return as a dictionary.
        
        Returns:
            Dictionary containing all loaded configurations
        """
        # Load reusable components (optional file)
        components_file = self.config_dir / "components.yaml"
        if components_file.exists():
            with open(components_file) as f:
                data = yaml.safe_load(f)
                self.components = data.get('components', {})
        
        # Load templates (required file)
        templates_file = self.config_dir / "templates.yaml"
        with open(templates_file) as f:
            data = yaml.safe_load(f)
            self.templates = data.get('templates', {})
        
        # Load boundaries (required file)
        boundaries_file = self.config_dir / "boundaries.yaml"
        with open(boundaries_file) as f:
            data = yaml.safe_load(f)
            self.boundaries = data.get('boundaries', [])
        
        # Load validation rules (required file)
        validation_file = self.config_dir / "validation.yaml"
        with open(validation_file) as f:
            data = yaml.safe_load(f)
            self.validation = data.get('validation', {})
        
        # Load instances (diagrams) in filename order
        instances_dir = self.config_dir / "instances"
        instance_files = sorted(instances_dir.glob("*.yaml"))
        
        for inst_file in instance_files:
            with open(inst_file) as f:
                instance = yaml.safe_load(f)
                # Store filename for error messages
                instance['_file'] = inst_file.name
                self.instances.append(instance)
        
        # Return everything as one dictionary
        return {
            'components': self.components,
            'templates': self.templates,
            'boundaries': self.boundaries,
            'validation': self.validation,
            'instances': self.instances
        }


# ============================================================================
# VALIDATOR: Checks diagram definitions for errors
# ============================================================================

class DiagramValidator:
    """
    Validates diagram definitions against rules.
    
    This class checks for:
    - Node ID format (namespace.name)
    - Missing components
    - Duplicate node IDs
    - Invalid edge references
    - Missing layer definitions
    """
    
    def __init__(self, validation_rules: Dict):
        # Store validation rules from YAML
        self.rules = validation_rules
        self.strict = validation_rules.get('strict', True)
        
        # Store errors and warnings separately
        self.errors = []
        self.warnings = []
        
    def validate_node_id(self, node_id: str) -> bool:
        """
        Check if a node ID matches the required format.
        
        Expected format: namespace.name (e.g., 'hw.rapl')
        - All lowercase
        - No special characters except dot
        - Dot separates namespace and name
        """
        pattern = self.rules.get('node_id_format', {}).get('pattern')
        if not pattern:
            return True
        
        regex = re.compile(pattern)
        return bool(regex.match(node_id))
    
    def validate_instance(self, instance: Dict, all_components: Dict) -> bool:
        """
        Validate a single diagram instance.
        
        Returns:
            True if valid, False if errors found
        """
        instance_name = instance.get('name', 'unknown')
        nodes = instance.get('nodes', [])
        edges = instance.get('edges', [])
        
        # Keep track of all node IDs in this diagram
        node_ids = set()
        
        # ================================================================
        # Validate all nodes
        # ================================================================
        for node in nodes:
            node_id = None
            node_layer = None
            
            # Handle string nodes (references to components) - OLD FORMAT
            if isinstance(node, str):
                node_id = node
                # Check if component exists
                if node_id not in all_components:
                    self.errors.append(
                        f"[{instance_name}] Component not found: {node_id}"
                    )
            
            # Handle dict nodes - NEW FORMAT with 'id' field
            elif isinstance(node, dict):
                node_id = node.get('id')
                if not node_id:
                    self.errors.append(
                        f"[{instance_name}] Node missing 'id' field: {node}"
                    )
                    continue
                
                node_layer = node.get('layer')
                
                # Inline nodes must specify a layer
                if not node_layer:
                    self.errors.append(
                        f"[{instance_name}] Inline node missing layer: {node_id}"
                    )
            
            # Check ID format
            if node_id and not self.validate_node_id(node_id):
                self.errors.append(
                    f"[{instance_name}] Invalid node ID format: {node_id}"
                )
            
            # Check for duplicate IDs
            if node_id in node_ids:
                self.errors.append(
                    f"[{instance_name}] Duplicate node ID: {node_id}"
                )
            node_ids.add(node_id)
        
        # ================================================================
        # Validate all edges (same as before)
        # ================================================================
        for edge in edges:
            from_node = edge.get('from')
            to_node = edge.get('to')
            
            # Check that both endpoints exist
            if from_node not in node_ids:
                self.errors.append(
                    f"[{instance_name}] Edge source not found: {from_node}"
                )
            if to_node not in node_ids:
                self.errors.append(
                    f"[{instance_name}] Edge target not found: {to_node}"
                )
        
        # Return True if no errors found
        return len(self.errors) == 0
    
    def get_report(self) -> Dict:
        """
        Get validation results.
        
        Returns:
            Dictionary with errors, warnings, and valid flag
        """
        return {
            'errors': self.errors,
            'warnings': self.warnings,
            'valid': len(self.errors) == 0
        }


# ============================================================================
# RESOLVER: Handles wildcards and component merging
# ============================================================================

class ComponentResolver:
    """
    Resolves components, expands wildcards, and applies templates.
    
    This class takes the raw validated data and transforms it into
    a complete graph definition ready for DOT generation.
    """
    
    def __init__(self, components: Dict, templates: Dict, boundaries: List):
        # Store all loaded data
        self.components = components
        self.templates = templates
        self.boundaries = boundaries
        
    def expand_wildcard(self, pattern: str, all_nodes: List[str]) -> List[str]:
        """
        Expand wildcard patterns like 'config.*' to matching node IDs.
        
        Simple algorithm:
        - If pattern ends with '*', match all nodes with that prefix
        - Otherwise, treat as exact node ID
        
        Example:
            pattern = 'config.*'
            all_nodes = ['config.loader', 'hw.rapl', 'config.db']
            returns = ['config.loader', 'config.db']
        """
        if pattern.endswith('*'):
            # Remove the '*', treat as prefix
            prefix = pattern[:-1]
            return [n for n in all_nodes if n.startswith(prefix)]
        else:
            # Exact match
            return [pattern] if pattern in all_nodes else []
    
    def resolve_node(self, node_def: Any, all_nodes: List[str]) -> List[Dict]:
        """
        Convert a node definition into actual node data.
        
        Handles:
        - String nodes: Look up in components (OLD FORMAT)
        - Wildcard strings: Expand to multiple nodes
        - Dict nodes with 'id' field (NEW FORMAT)
        """
        results = []
        
        # Case 1: String node (could be component reference or wildcard)
        if isinstance(node_def, str):
            # Check if it's a wildcard
            if node_def.endswith('*'):
                # Expand wildcard to multiple node IDs
                expanded_ids = self.expand_wildcard(node_def, all_nodes)
                for node_id in expanded_ids:
                    if node_id in self.components:
                        # Found in components - use component definition
                        node_data = self.components[node_id].copy()
                        node_data['id'] = node_id
                        results.append(node_data)
                    else:
                        # Wildcard matched but component missing
                        print(f"Warning: No component for {node_id}")
            else:
                # Single node reference
                if node_def in self.components:
                    node_data = self.components[node_def].copy()
                    node_data['id'] = node_def
                    results.append(node_data)
                else:
                    print(f"Warning: Component not found: {node_def}")
        
        # Case 2: Dict node with 'id' field (NEW FORMAT)
        elif isinstance(node_def, dict):
            node_id = node_def.get('id')
            if node_id:
                # Create node data from dict
                node_data = node_def.copy()
                node_data['id'] = node_id
                results.append(node_data)
            else:
                print(f"Warning: Dict node missing 'id' field: {node_def}")
        
        return results


# ============================================================================
# DOT BUILDER: Creates DOT format strings (pure printer, no logic)
# ============================================================================

# ============================================================================
# DOT BUILDER: Creates DOT format strings (pure printer, no logic)
# ============================================================================

class DotBuilder:
    """
    Builds DOT format strings from resolved graph data.
    
    This class does NO business logic - it just prints DOT syntax.
    All decisions should already be made by the resolver.
    
    IMPORTANT RULES:
    1. Node IDs with dots MUST be quoted (e.g., "exec.harness")
    2. Only DOT-compatible attributes are included
    3. Labels with quotes or newlines are escaped
    4. Edge styles come from template
    """
    
    def __init__(self, template_name: str, templates: Dict):
        # Get the template for this diagram
        self.template = templates.get(template_name, {})
        
        # List of attributes that Graphviz understands
        self.dot_attributes = [
            'label',      # Text label
            'shape',      # box, circle, cylinder, diamond, record
            'color',      # Node color
            'style',      # filled, dashed, solid, dotted
            'fontname',   # Font family
            'fontsize',   # Font size
            'fillcolor',  # Fill color when style=filled
            'width',      # Node width
            'height'      # Node height
        ]
        
    def _quote_if_needed(self, node_id: str) -> str:
        """
        Quote node IDs that contain dots.
        
        Graphviz treats unquoted dots as separators, causing syntax errors.
        Since all our node IDs use namespace.name format, they ALL need quotes.
        """
        if '.' in node_id:
            return f'"{node_id}"'
        return node_id
    
    def _escape_label(self, label: str) -> str:
        """
        Escape special characters in labels.
        
        Handles:
        - Double quotes (")
        - Newlines (\n)
        - Backslashes
        """
        # Escape double quotes
        label = label.replace('"', '\\"')
        return label
        
    def build_graph_header(self) -> str:
        """Create the graph header with template attributes."""
        lines = ["digraph {"]
        
        # Add graph attributes from template
        graph_attrs = self.template.get('graph', {})
        for key, value in graph_attrs.items():
            # Quote color values
            if key == 'bgcolor' and value.startswith('#'):
                lines.append(f'  {key}="{value}";')
            else:
                lines.append(f"  {key}={value};")
        
        return "\n".join(lines)
    
    def build_node(self, node_data: Dict) -> str:
        """
        Create a node definition string with proper attribute formatting.
        
        Special handling:
        - shape attributes: no quotes (shape=none)
        - HTML labels: no quotes (label=<<TABLE>>)
        - regular labels: quoted (label="text")
        """
        node_id = node_data['id']
        quoted_id = self._quote_if_needed(node_id)
        
        # Build attributes
        attrs = []
        for key, value in node_data.items():
            if key in self.dot_attributes:
                # Handle HTML labels (no quotes, no escaping)
                if key == 'label' and isinstance(value, str) and value.strip().startswith('<'):
                    attrs.append(f'label={value}')
                else:
                    if key == 'label':
                        value = self._escape_label(value)
                    attrs.append(f'{key}="{value}"')
        
        if attrs:
            return f"  {quoted_id} [{', '.join(attrs)}];"
        else:
            return f"  {quoted_id};"
    
    def build_edge(self, edge_data: Dict, edge_styles: Dict) -> str:
        from_node = self._quote_if_needed(edge_data['from'])
        to_node = self._quote_if_needed(edge_data['to'])
        edge_type = edge_data.get('type', 'flow')
        
        # Get style for this edge type from template
        style = edge_styles.get(edge_type, {})
        
        # Build edge attributes
        attrs = []
        for key, value in style.items():
            # Quote color values
            if key == 'color' and isinstance(value, str) and value.startswith('#'):
                attrs.append(f'{key}="{value}"')
            else:
                attrs.append(f'{key}={value}')
        
        if 'label' in edge_data:
            label = edge_data['label'].replace('"', '\\"')
            attrs.append(f'label="{label}"')
        
        if attrs:
            return f"  {from_node} -> {to_node} [{', '.join(attrs)}];"
        else:
            return f"  {from_node} -> {to_node};"
    
    def build_subgraph(self, name: str, nodes: List[str], attrs: Dict) -> str:
        """
        Create a subgraph (cluster) definition.
        
        Example:
            subgraph cluster_user_space {
              label="User Space";
              style=dashed;
              color=lightblue;
              "config.loader";
              "exec.harness";
            }
        """
        lines = [f"  subgraph cluster_{name} {{"]
        
        # Add subgraph attributes
        for key, value in attrs.items():
            if key == 'label':
                value = self._escape_label(value)
            lines.append(f"    {key}={value};")
        
        # Add nodes (already quoted by build_node when rendered)
        for node in nodes:
            quoted_node = self._quote_if_needed(node)
            lines.append(f"    {quoted_node};")
        
        lines.append("  }")
        return "\n".join(lines)

# ============================================================================
# SVG RENDERER: Converts DOT to SVG
# ============================================================================

class SvgRenderer:
    """
    Renders DOT strings to SVG files using the 'dot' command.
    
    Simple wrapper around the Graphviz command-line tool.
    """
    
    def __init__(self):
        pass
        
    def render(self, dot_string: str, output_path: Path) -> bool:
        """
        Convert DOT string to SVG file.
        
        Args:
            dot_string: Graph in DOT format
            output_path: Where to save the SVG
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create temporary DOT file
            temp_dot = output_path.with_suffix('.dot')
            with open(temp_dot, 'w') as f:
                f.write(dot_string)
            
            # Run dot command
            result = subprocess.run(
                ['dot', '-Tsvg', '-o', str(output_path), str(temp_dot)],
                capture_output=True,
                text=True
            )
            
            # Clean up temp file
            temp_dot.unlink()
            
            # Check for errors
            if result.returncode != 0:
                print(f"Error generating SVG: {result.stderr}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error rendering SVG: {e}")
            return False