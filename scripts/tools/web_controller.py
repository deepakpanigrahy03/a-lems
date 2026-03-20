#!/usr/bin/env python3
"""
Web Controller for A-LEMS Developer Tools
Goal 6: Browser UI to run all tools with one click
Run: python scripts/tools/web_controller.py
"""

import argparse
import glob
import json
import shlex  # IMPORTANT: For handling quoted arguments
import sqlite3
import subprocess
import sys
import webbrowser
from pathlib import Path
from path_loader import config
from flask import send_from_directory

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from flask import Flask, jsonify, render_template_string, request
except ImportError:
    print("❌ Flask not installed. Run: pip install flask")
    sys.exit(1)

app = Flask(__name__)
repo_root = None

# ============================================================================
# Column Flow Analyzer (Goal 9)
# ============================================================================


class ColumnFlowAnalyzer:
    def __init__(self):
        self.repo_root = self._find_repo_root()
        self.config = self._load_config()
        self.conn = None
        self._connect_db()

    def _find_repo_root(self) -> Path:
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def _load_config(self):
        """Load database config from app_settings.yaml"""
        config_path = self.repo_root / "config" / "app_settings.yaml"
        if not config_path.exists():
            return {"engine": "sqlite", "sqlite": {"path": str(config.DB_PATH)}}

        with open(config_path) as f:
            config = yaml.safe_load(f)

        return config.get("database", {})

    def _connect_db(self):
        """Connect to database based on config"""
        engine = self.config.get("engine", "sqlite")

        if engine == "sqlite":
            db_path = self.config.get("sqlite", {}).get("path", str(config.DB_PATH))
            full_path = self.repo_root / db_path
            self.conn = sqlite3.connect(full_path)
            self.conn.row_factory = sqlite3.Row

    def get_tables(self):
        """Get list of all tables"""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table):
        """Get columns for a table"""
        cursor = self.conn.execute(f"PRAGMA table_info({table})")
        return [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]


# ============================================================================
# HTML Template
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>A-LEMS Developer Control Center</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        .dashboard {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h3 {
            margin-top: 0;
            color: #3498db;
        }
        button {
            background: #3498db;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin: 5px;
        }
        button:hover {
            background: #2980b9;
        }
        button:disabled {
            background: #95a5a6;
            cursor: not-allowed;
        }
        select, input {
            width: 100%;
            padding: 8px;
            margin: 5px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        pre {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
            font-size: 12px;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .output {
            margin-top: 20px;
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .status {
            padding: 10px;
            border-radius: 4px;
            margin: 10px 0;
        }
        .success {
            background: #d4edda;
            color: #155724;
        }
        .error {
            background: #f8d7da;
            color: #721c24;
        }
        .loading {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f3f3;
            border-top: 3px solid #3498db;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .tooltip {
            position: relative;
            display: inline-block;
        }
        .tooltip .tooltiptext {
            visibility: hidden;
            background-color: #555;
            color: #fff;
            text-align: center;
            padding: 5px;
            border-radius: 6px;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            margin-left: -60px;
            width: 120px;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 A-LEMS Developer Control Center</h1>
        
        <div class="dashboard">

<!-- Goal 1: LLM Context Generator -->
<div class="card">
    <h3>📋 LLM Context Generator</h3>
    <p class="description">Get complete code context to paste into Claude/DeepSeek for automatic code changes</p>
    <div class="help-text">Type what you want to change (e.g., "add page_faults column to runs table")</div>
    <input type="text" id="change_request" placeholder="e.g., add page_faults to runs table" style="width: 100%; padding: 8px; margin: 10px 0;">
    <button onclick="runTool('llm_context', '--change \'' + document.getElementById('change_request').value + '\'')">🔍 Generate Context</button>
    <div class="tooltip">This will analyze your codebase and give you everything needed to ask an AI for the exact code changes</div>
</div>

<!-- Goal 2: Sphinx API Documentation -->
<div class="card">
    <h3>📚 Complete API Documentation (Sphinx)</h3>
    <p>Auto-generated API reference for the ENTIRE project:</p>
    <p style="font-size:0.9em; color:#555;">✓ core/ (energy measurement) ✓ scripts/ (9 tools) ✓ gui/ (dashboard) ✓ migrations/ ✓ tests/</p>
    <button onclick="runTool('sphinx_docs', '--build')">📖 Build HTML Docs</button>
    <button onclick="runTool('sphinx_docs', '--pdf')">📄 Build PDF</button>
    <button onclick="window.open('/sphinx-docs/index.html', '_blank')">🔍 Open Docs</button>
    <div class="help-text">Complete API reference for ALL modules</div>
</div>

<!-- MkDocs User Documentation -->
<div class="card">
    <h3>📘 User Guide (MkDocs)</h3>
    <p class="description">Generate user-friendly tutorials and guides</p>
    <button onclick="runTool('mkdocs_docs', '--build')">🏗️ Build Docs</button>
   <button onclick="window.open('/docs/generated/mkdocs/index.html', '_blank')">👁️ View Docs</button>
    <button onclick="runTool('mkdocs_docs', '--serve-bg')">▶️ Start Server (port 8000)</button>
    <button onclick="runTool('mkdocs_docs', '--stop')">⏹️ Stop Server</button>
    <div class="help-text">For end-users: how to run experiments and understand results</div>
</div>

<!-- Goal 3: Code Quality Checker -->
<div class="card">
    <h3>🔍 Code Quality Guardian</h3>
    <p class="description">Find bugs, complexity issues, and style problems</p>
    <button onclick="runTool('quality_check', '')">✅ Run All Checks</button>
    <button onclick="runTool('quality_check', '--verbose')">📊 Detailed Report</button>
    <div class="help-text">Checks: pylint, mypy, complexity, security, dead code, formatting</div>
</div>

<!-- Goal 4: Refactoring Advisor -->
<div class="card">
    <h3>🔄 Refactoring Advisor</h3>
    <p class="description">Get suggestions to improve code structure</p>
    <input type="text" id="refactor_target" placeholder="e.g., core/execution/harness.py" style="width: 100%; padding: 8px; margin: 10px 0;">
    <button onclick="runTool('refactor_advisor', '--target ' + document.getElementById('refactor_target').value)">💡 Analyze</button>
    <div class="help-text">Finds duplicate code, long functions, and suggests how to split them</div>
</div>

<!-- Goal 5: Team Dashboard -->
<div class="card">
    <h3>👥 Team Collaboration Dashboard</h3>
    <p class="description">See who wrote what and identify knowledge gaps</p>
    <button onclick="runTool('team_dashboard', '')">📅 Last 30 days</button>
    <button onclick="runTool('team_dashboard', '--days 90')">📆 Last 90 days</button>
    <div class="help-text">Shows bus factor: which files only one person understands</div>
</div>

<!-- Goal 7: Requirements Verifier -->
<div class="card">
    <h3>✅ Requirements Verifier</h3>
    <p class="description">Check if code matches specifications</p>
    <button onclick="runTool('verify_requirements', '')">🔎 Verify All</button>
    <div class="help-text">Shows what's implemented, partially done, and completely missing</div>
</div>

<!-- Goal 9: Column Flow Analyzer -->
<div class="card">
    <h3>📊 Column Data Flow Tracker</h3>
    <p class="description">Trace how a database column gets its data</p>
    <select id="table_select" style="width: 100%; padding: 8px; margin: 5px 0;"></select>
    <select id="column_select" style="width: 100%; padding: 8px; margin: 5px 0;" disabled></select>
    <button onclick="traceColumn()" id="trace_btn" disabled>🔍 Trace Flow</button>
    <div class="help-text">Shows every file and line number that touches this column</div>
</div>

<!-- Goal 10: Issue Auto-Diagnoser -->
<div class="card">
    <h3>🔬 Issue Auto-Diagnoser</h3>
    <p class="description">Run ALL tools and find root causes automatically</p>
    <button class="tool-button" onclick="runTool('issue_tracer', '')">🔍 Diagnose Issues</button>
    <div class="help-text">Correlates quality checks, docs, database, and web controller</div>
</div>
<!-- Goal: Diagram Generator -->
<div class="card">
    <h3>🎨 Diagram Generator</h3>
    <p class="description">Generate architecture and dependency diagrams</p>
    <button onclick="runTool('generate_diagrams', '')">📊 Generate All Diagrams</button>
    <button onclick="runTool('generate_diagrams', '--name architecture')">🏗️ Architecture Only</button>
    <div class="help-text">Creates SVG files in docs/assets/diagrams/</div>
</div>

        </div>
        
        <div class="output" id="output">
            <h3>Output</h3>
            <div id="output_content">Ready. Click a button to run a tool.</div>
        </div>
    </div>
    
    <script>
        // Load tables for Goal 9 on page load
        fetch('/get_tables')
            .then(r => r.json())
            .then(data => {
                const select = document.getElementById('table_select');
                data.tables.forEach(t => {
                    const option = document.createElement('option');
                    option.value = t;
                    option.textContent = t;
                    select.appendChild(option);
                });
            });
        
        // When table selected, load columns
        document.getElementById('table_select').onchange = function() {
            const table = this.value;
            if (!table) {
                document.getElementById('column_select').disabled = true;
                document.getElementById('trace_btn').disabled = true;
                return;
            }
            
            fetch(`/get_columns?table=${table}`)
                .then(r => r.json())
                .then(data => {
                    const colSelect = document.getElementById('column_select');
                    colSelect.innerHTML = '<option value="">-- Select Column --</option>';
                    data.columns.forEach(c => {
                        const option = document.createElement('option');
                        option.value = c.name;
                        option.textContent = `${c.name} (${c.type})`;
                        colSelect.appendChild(option);
                    });
                    colSelect.disabled = false;
                });
        };
        
        // When column selected, enable trace button
        document.getElementById('column_select').onchange = function() {
            document.getElementById('trace_btn').disabled = !this.value;
        };
        
        // Trace function for Goal 9
        function traceColumn() {
            const table = document.getElementById('table_select').value;
            const column = document.getElementById('column_select').value;
            runTool('column_flow', `--table ${table} --column ${column}`);
        }
        
        // Main function to run any tool
        async function runTool(tool, args) {
            const outputDiv = document.getElementById('output_content');
            outputDiv.innerHTML = '<div class="loading"></div> Running...';
            
            try {
                const response = await fetch('/run', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({tool: tool, args: args})
                });
                
                const data = await response.json();
                
                if (data.success) {
                    outputDiv.innerHTML = '<div class="status success">✅ Success</div><pre>' + 
                        escapeHtml(data.output) + '</pre>';
                } else {
                    outputDiv.innerHTML = '<div class="status error">❌ Error</div><pre>' + 
                        escapeHtml(data.error) + '</pre>';
                }
            } catch (error) {
                outputDiv.innerHTML = '<div class="status error">❌ Connection error</div>';
            }
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    </script>
</body>
</html>
"""

# ============================================================================
# Flask Routes
# ============================================================================


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/get_tables")
def get_tables():
    analyzer = ColumnFlowAnalyzer()
    return jsonify({"tables": analyzer.get_tables()})


@app.route("/get_columns")
def get_columns():
    table = request.args.get("table")
    analyzer = ColumnFlowAnalyzer()
    return jsonify({"columns": analyzer.get_columns(table)})


@app.route("/docs/<path:filename>")
def serve_docs(filename):
    """Serve documentation files without authentication"""
    from flask import send_from_directory

    site_dir = ROOT / "site"
    return send_from_directory(str(site_dir), filename)

@app.route('/sphinx-docs/<path:filename>')
def serve_sphinx_docs(filename):
    """Serve Sphinx documentation"""
    from flask import send_from_directory
    site_dir = ROOT / 'docs' / 'build' / 'html'
    print(f"🔍 Serving from: {site_dir}")
    print(f"🔍 File requested: {filename}")
    print(f"🔍 File exists? {(site_dir / filename).exists()}")
    return send_from_directory(str(site_dir), filename)

@app.route('/docs/')
@app.route('/docs/<path:filename>')
def serve_mkdocs_docs(filename='index.html'):
    """Serve MkDocs generated documentation"""
    from flask import send_from_directory
    from pathlib import Path

    site_dir = ROOT / 'docs' / 'generated' / 'mkdocs'
    
    # Handle trailing slash (directory request)
    if filename.endswith('/'):
        filename = filename.rstrip('/') + '/index.html'
    
    file_path = site_dir / filename

    # If directory → serve index.html inside it
    if file_path.is_dir():
        return send_from_directory(str(file_path), 'index.html')

    # If file exists → serve it
    if file_path.exists():
        return send_from_directory(str(site_dir), filename)

    return "File not found", 404
    
@app.route("/run", methods=["POST"])
def run_tool():
    data = request.json
    tool = data.get("tool")
    args = data.get("args", "")

    tool_scripts = {
        "llm_context": "llm_context.py",
        "sphinx_docs": "sphinx_docs.py",
        "mkdocs_docs": "mkdocs_docs.py",
        "quality_check": "quality_check.py",
        "refactor_advisor": "refactor_advisor.py",
        "team_dashboard": "team_dashboard.py",
        "verify_requirements": "verify_requirements.py",
        "column_flow": "column_flow.py",  # New Goal 9
        "issue_tracer": "issue_tracer.py",
        "generate_diagrams": "generate_diagrams.py",  # New Goal 10

    }

    if tool not in tool_scripts:
        return jsonify({"success": False, "error": f"Unknown tool: {tool}"})

    script_path = Path(__file__).parent / tool_scripts[tool]
    if not script_path.exists():
        return jsonify({"success": False, "error": f"Tool not found: {script_path}"})

    try:
        cmd = [sys.executable, str(script_path)]
        if args:
            # Use shlex to properly handle quoted arguments
            cmd.extend(shlex.split(args))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            return jsonify({"success": True, "output": result.stdout})
        else:
            return jsonify({"success": False, "error": result.stderr or result.stdout})
    except subprocess.TimeoutExpired:
        return jsonify({"success": False, "error": "Command timed out"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


def main():
    parser = argparse.ArgumentParser(
        description="Launch web controller for A-LEMS tools"
    )
    parser.add_argument("--port", type=int, default=8888, help="Port to run on")
    parser.add_argument(
        "--no-browser", action="store_true", help="Don't open browser automatically"
    )

    args = parser.parse_args()

    url = f"http://localhost:{args.port}"

    print(f"🚀 Starting A-LEMS Web Controller on {url}")
    print("Press Ctrl+C to stop")

    if not args.no_browser:
        webbrowser.open(url)

    app.run(host="127.0.0.1", port=args.port, debug=False)


if __name__ == "__main__":
    main()
