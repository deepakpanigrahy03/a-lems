
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
# html_static_path = ['_static']

# Output to docs/generated/sphinx/
html_static_path = ['_static']
html_theme = 'sphinx_rtd_theme'
