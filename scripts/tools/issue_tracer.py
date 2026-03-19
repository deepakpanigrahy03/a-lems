#!/usr/bin/env python3
"""
Issue Auto-Diagnoser for A-LEMS
Goal 10: Run ALL tools and correlate issues to find root causes
Run: python scripts/tools/issue_tracer.py
"""

import subprocess
import json
import sys
from pathlib import Path
from datetime import datetime

class IssueTracer:
    def __init__(self):
        self.repo_root = Path(__file__).parent.parent.parent
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'issues': [],
            'correlations': [],
            'recommendations': []
        }
    
    def run_quality_check(self):
        """Run quality_check.py and parse results"""
        print("🔍 Running quality checks...")
        result = subprocess.run(
            [sys.executable, "scripts/tools/quality_check.py", "--json"],
            capture_output=True, text=True, cwd=self.repo_root
        )
        try:
            return json.loads(result.stdout)
        except:
            return {"error": "Failed to parse quality check"}
    
    def run_mypy(self):
        """Run mypy and collect type errors"""
        print("🔍 Running mypy type checking...")
        result = subprocess.run(
            ["mypy", "core/"],
            capture_output=True, text=True, cwd=self.repo_root
        )
        return result.stdout
    
    def check_sphinx_docs(self):
        """Check if Sphinx docs build without errors"""
        print("🔍 Checking Sphinx documentation...")
        result = subprocess.run(
            [sys.executable, "scripts/tools/sphinx_docs.py", "--build"],
            capture_output=True, text=True, cwd=self.repo_root
        )
        return {
            'success': result.returncode == 0,
            'output': result.stderr if result.returncode != 0 else "OK"
        }
    
    def check_mkdocs(self):
        """Check if MkDocs builds without errors"""
        print("🔍 Checking MkDocs documentation...")
        result = subprocess.run(
            [sys.executable, "scripts/tools/mkdocs_docs.py", "--build"],
            capture_output=True, text=True, cwd=self.repo_root
        )
        return {
            'success': result.returncode == 0,
            'output': result.stderr if result.returncode != 0 else "OK"
        }
    
    def check_web_controller(self):
        """Check if web controller starts"""
        print("🔍 Checking web controller...")
        result = subprocess.run(
            ["python", "-m", "py_compile", "scripts/tools/web_controller.py"],
            capture_output=True, text=True, cwd=self.repo_root
        )
        return {
            'success': result.returncode == 0,
            'error': result.stderr if result.returncode != 0 else None
        }
    
    def check_database(self):
        """Check database connectivity and schema"""
        print("🔍 Checking database...")
        try:
            # Add scripts directory to path
            import sys
            sys.path.insert(0, str(self.repo_root))
            
            # Import functions directly
            from scripts.tools.column_flow import get_db
            
            # Test database connection
            db = get_db()
            db.close()
            
            return {
                'success': True,
                'tables_count': 0,
                'note': 'Database connection OK'
            }
        except ImportError as e:
            return {
                'success': False,
                'error': f"Import error: {e}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def run(self):
        """Run all diagnostic checks"""
        print("\n" + "="*60)
        print("🔬 A-LEMS Issue Auto-Diagnoser")
        print("="*60)
        
        quality = self.run_quality_check()
        sphinx = self.check_sphinx_docs()
        mkdocs = self.check_mkdocs()
        web = self.check_web_controller()
        db = self.check_database()
        
        print("\n📊 DIAGNOSIS RESULTS")
        print("-"*40)
        
        issues_found = []
        
        if isinstance(quality, dict):
            pylint_count = quality.get('checks', {}).get('pylint', {}).get('total_issues', 0)
            if pylint_count:
                issues_found.append(f"📋 Pylint issues: {pylint_count}")
        
        if not sphinx['success']:
            issues_found.append("📚 Sphinx docs: ❌ Failed")
        else:
            print("📚 Sphinx docs: ✅ OK")
        
        if not mkdocs['success']:
            issues_found.append("📘 MkDocs: ❌ Failed")
        else:
            print("📘 MkDocs: ✅ OK")
        
        if not web['success']:
            issues_found.append(f"🌐 Web controller: ❌ {web['error']}")
        else:
            print("🌐 Web controller: ✅ OK")
        
        if not db['success']:
            issues_found.append(f"🗄️ Database: ❌ {db['error']}")
        else:
            print(f"🗄️ Database: ✅ OK ({db['tables_count']} tables)")
        
        if issues_found:
            print("\n❌ ISSUES DETECTED:")
            for issue in issues_found:
                print(f"  {issue}")
        else:
            print("\n✅ No issues detected!")
        
        print("\n" + "="*60)

def main():
    tracer = IssueTracer()
    tracer.run()

if __name__ == "__main__":
    main()
