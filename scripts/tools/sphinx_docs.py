#!/usr/bin/env python3
"""
Sphinx Documentation Generator for A-LEMS
Goal 2: Generate complete API documentation from code
Run: python scripts/tools/sphinx_docs.py --build
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from path_loader import config


class SphinxDocsGenerator:
    def __init__(self, output_dir="docs", clean=False):
        self.repo_root = self._find_repo_root()
        self.docs_dir = self.repo_root / output_dir
        self.source_dir = config.SPHINX_SOURCE
        self.build_dir = config.SPHINX_OUTPUT
        self.clean = clean

    def _find_repo_root(self) -> Path:
        """Find repository root (where .git is)"""
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def check_sphinx_installed(self) -> bool:
        """Check if sphinx is installed"""
        return shutil.which("sphinx-build") is not None

    def setup_sphinx(self) -> bool:
        """Initialize Sphinx documentation structure"""
        if not self.docs_dir.exists():
            self.docs_dir.mkdir(parents=True)
        
        # Create conf.py if it doesn't exist
        conf_py = self.source_dir / "conf.py"
        if not conf_py.exists():
            print("📝 Creating Sphinx configuration...")
            self.source_dir.mkdir(exist_ok=True)
            
            conf_content = '''
# Configuration file for the Sphinx documentation builder
import os
import sys
sys.path.insert(0, os.path.abspath('../../'))

project = 'A-LEMS'
copyright = '2026, A-LEMS Team'
author = 'A-LEMS Team'
release = '1.0.0'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.graphviz',
    'sphinx_rtd_theme',
]

templates_path = ['_templates']
exclude_patterns = []
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
'''
            with open(conf_py, 'w') as f:
                f.write(conf_content)
            
            # Create index.rst with ALL modules
            index_rst = self.source_dir / "index.rst"
            index_content = '''
Welcome to A-LEMS's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   core
   scripts
   gui
   migrations
   tests

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
'''
            with open(index_rst, 'w') as f:
                f.write(index_content)
            
            return True
        return False

    def generate_api_docs(self):
        """Generate API documentation from code - AUTO-SCANS all packages"""
        print("🔍 Scanning project for Python packages...")
        
        # Auto-discover all Python packages in the project
        packages = []
        for item in self.repo_root.iterdir():
            if item.is_dir() and (item / "__init__.py").exists():
                packages.append(str(item))
                print(f"   📦 Found package: {item.name}")
        
        # Also include key directories even without __init__.py
        extra_dirs = ["scripts", "migrations", "tests"]
        for dir_name in extra_dirs:
            dir_path = self.repo_root / dir_name
            if dir_path.exists() and dir_path.is_dir():
                packages.append(str(dir_path))
                print(f"   📁 Found directory: {dir_name}")
        
        print(f"🔍 Generating API documentation for {len(packages)} packages...")
        
        # Run sphinx-apidoc for all discovered packages
        cmd = [
            "sphinx-apidoc",
            "-o", str(self.source_dir),
            "-f",  # Force overwrite
            "-e",  # Put each module in its own page
            "--no-toc",  # Don't create module index
        ] + packages
        
        subprocess.run(cmd, check=False)
        
        # Update index.rst with all discovered packages
        self._update_index_rst(packages)
    
    def _update_index_rst(self, packages):
        """Update index.rst to include all discovered packages"""
        index_rst = self.source_dir / "index.rst"
        
        # Extract just the folder names from package paths
        module_names = [Path(p).name for p in packages]
        
        content = f'''Welcome to A-LEMS's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

{chr(10).join([f'   {name}' for name in module_names])}

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
'''
        with open(index_rst, 'w') as f:
            f.write(content)
        
        print(f"✅ Updated index.rst with {len(module_names)} modules")

    def build_html(self):
        """Build HTML documentation"""
        print("📚 Building HTML documentation...")

        if self.clean and self.build_dir.exists():
            shutil.rmtree(self.build_dir)

        result = subprocess.run(
            [
                "sphinx-build",
                "-b",
                "html",
                str(self.source_dir),
                str(self.build_dir),  
                "-a",
                "-E",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print(
                f"✅ HTML documentation built at {self.build_dir / 'index.html'}" 
            )
        else:
            print("❌ Build failed:")
            print(result.stderr)

        return result.returncode == 0

    def build_pdf(self):
        """Build PDF documentation (requires LaTeX)"""
        print("📄 Building PDF documentation...")

        # First build LaTeX
        latex_dir = self.build_dir / "latex"
        if self.clean and latex_dir.exists():
            shutil.rmtree(latex_dir)

        result = subprocess.run(
            [
                "sphinx-build",
                "-b",
                "latex",
                str(self.source_dir),
                str(latex_dir),
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print("❌ LaTeX build failed")
            return False

        # Then build PDF
        print("   Running LaTeX to generate PDF...")
        result = subprocess.run("make", cwd=latex_dir, capture_output=True, text=True)

        pdf_path = latex_dir / "A-LEMS.pdf"
        if pdf_path.exists():
            print(f"✅ PDF documentation built at {pdf_path}")
            return True
        else:
            print("❌ PDF generation failed (LaTeX may not be installed)")
            return False

    def open_docs(self):
        """Open documentation in browser"""
        index_path = self.build_dir / "html" / "index.html"
        if index_path.exists():
            import webbrowser

            webbrowser.open(f"file://{index_path.absolute()}")
            print(f"🌐 Opened {index_path}")
        else:
            print("❌ Documentation not built yet. Run --build first.")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Sphinx documentation for A-LEMS"
    )
    parser.add_argument(
        "--setup", action="store_true", help="Initialize Sphinx structure"
    )
    parser.add_argument("--build", action="store_true", help="Build HTML documentation")
    parser.add_argument(
        "--pdf", action="store_true", help="Build PDF documentation (requires LaTeX)"
    )
    parser.add_argument(
        "--open", action="store_true", help="Open documentation in browser"
    )
    parser.add_argument(
        "--clean", action="store_true", help="Clean build directory before building"
    )
    parser.add_argument(
        "--output", "-o", default="docs", help="Output directory (default: docs)"
    )

    args = parser.parse_args()

    generator = SphinxDocsGenerator(output_dir=args.output, clean=args.clean)

    # Check if sphinx is installed
    if not generator.check_sphinx_installed():
        print("❌ Sphinx not found. Install with:")
        print("   pip install sphinx sphinx-rtd-theme")
        sys.exit(1)

    # Setup if requested
    if args.setup:
        if generator.setup_sphinx():
            print("✅ Sphinx structure created")
            generator.generate_api_docs()

    # Build if requested
    if args.build:
        generator.generate_api_docs()
        generator.build_html()

    # Build PDF if requested
    if args.pdf:
        generator.generate_api_docs()
        generator.build_pdf()

    # Open if requested
    if args.open:
        generator.open_docs()

    # If no action specified, show help
    if not any([args.setup, args.build, args.pdf, args.open]):
        parser.print_help()


if __name__ == "__main__":
    main()
