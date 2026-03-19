#!/usr/bin/env python3
"""
Path configuration loader - loads from config/paths.yaml
Supports your hierarchical YAML structure
"""
import yaml
from pathlib import Path

class PathConfig:
    def __init__(self, config_file=None):
        if config_file is None:
            self.config_file = Path(__file__).parent.parent.parent / "config" / "paths.yaml"
        else:
            self.config_file = Path(config_file)
        
        self.load()
        self.load_db_path()
    
    def load(self):
        with open(self.config_file) as f:
            self.config = yaml.safe_load(f)
        
        # Project root
        project_root = Path(__file__).parent.parent.parent
        
        # ====================================================================
        # Load project metadata
        # ====================================================================
        project = self.config.get('project', {})
        self.PROJECT_NAME = project.get('name', 'A-LEMS')
        self.REPO_URL = project.get('repo_url', 'https://github.com/deepakpanigrahy03/a-lems')
        self.AUTHOR = project.get('author', 'A-LEMS Team')
        self.DESCRIPTION = project.get('description', 'Agent vs Linear AI Energy Measurement Platform')
        
        # ====================================================================
        # Load paths from your hierarchical structure
        # ====================================================================
        
        # Documentation paths
        docs = self.config['docs']
        self.GUIDES_PATH = project_root / docs['guides']
        
        generated = docs['generated']
        self.API_OUTPUT = project_root / generated['api']
        self.SPHINX_OUTPUT = project_root / generated['sphinx']
        self.MKDOCS_OUTPUT = project_root / generated['mkdocs']
        
        self.ASSETS_PATH = project_root / docs['assets']
        self.DIAGRAMS_OUTPUT = project_root / docs['diagrams']
        self.MKDOCS_DIAGRAMS = project_root / docs['mkdocs_diagrams']
        
        # Sources
        sources = self.config['sources']
        self.SPHINX_SOURCE = project_root / sources['sphinx']['source']
        self.SPHINX_CONFIG = project_root / sources['sphinx']['config']
        self.MKDOCS_SOURCE = project_root / sources['mkdocs']['source']
        self.MKDOCS_CONFIG = project_root / sources['mkdocs']['config']
        
        
        # Tool outputs
        tools = self.config['tools']
        self.TOOL_DIAGRAMS = project_root / tools['diagrams']
        self.TOOL_REPORTS = project_root / tools['reports']
        
        # Create directories
        for path in [self.GUIDES_PATH, self.API_OUTPUT, 
                     self.SPHINX_OUTPUT, self.MKDOCS_OUTPUT,
                     self.ASSETS_PATH, self.DIAGRAMS_OUTPUT,
                     self.SPHINX_SOURCE, self.MKDOCS_SOURCE,
                     self.TOOL_DIAGRAMS, self.TOOL_REPORTS]:
            path.mkdir(parents=True, exist_ok=True)

    def load_db_path(self):
        """
        Load database path from app_settings.yaml.
        """
        # Same pattern as in load() - compute locally
        project_root = Path(__file__).parent.parent.parent
        app_settings = project_root / "config" / "app_settings.yaml"
        
        try:
            with open(app_settings) as f:
                settings = yaml.safe_load(f)
            
            db_config = settings.get('database', {})
            
            # Handle SQLite configuration
            if db_config.get('engine') == 'sqlite':
                sqlite_config = db_config.get('sqlite', {})
                db_path = sqlite_config.get('path', 'data/experiments.db')
                self.DB_PATH = project_root / db_path
            else:
                self.DB_PATH = None
                print("⚠️  PostgreSQL database path not loaded")
                
        except FileNotFoundError:
            print(f"⚠️  Config file not found: {app_settings}")
            self.DB_PATH = project_root / "data" / "experiments.db"
        except Exception as e:
            print(f"⚠️  Error loading database path: {e}")
            self.DB_PATH = project_root / "data" / "experiments.db"    

    def __str__(self):
        return f"""PathConfig:
  Project: {self.PROJECT_NAME}
  Author: {self.AUTHOR}
  Repo: {self.REPO_URL}
  
  Guides: {self.GUIDES_PATH}
  API Output: {self.API_OUTPUT}
  Sphinx Output: {self.SPHINX_OUTPUT}
  MkDocs Output: {self.MKDOCS_OUTPUT}
  Diagrams: {self.DIAGRAMS_OUTPUT}
  Sphinx Source: {self.SPHINX_SOURCE}
  Database: {getattr(self, 'DB_PATH', 'Not configured')}
  MkDocs Source: {self.MKDOCS_SOURCE}"""

# Global instance
config = PathConfig()

if __name__ == "__main__":
    print(config)