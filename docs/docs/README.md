# A-LEMS Documentation

## 📚 Documentation Sections

| Section | Description | Location |
|---------|-------------|----------|
| **Quick Start** | 5-minute setup guide | [guides/quick-start.md](guides/quick-start.md) |
| **Getting Started** | Step-by-step setup | [guides/getting-started/](guides/getting-started/) |
| **User Guide** | Daily usage | [guides/user-guide/](guides/user-guide/) |
| **Developer Guide** | Contributing | [guides/developer-guide/](guides/developer-guide/) |
| **API Reference** | Auto-generated | [generated/api/](generated/api/) |
| **Database Schema** | Table definitions | [generated/database/](generated/database/) |

## 🛠️ Building Documentation

```bash
# Build all docs
./scripts/build-docs.sh
📖 Viewing Documentation
GitHub: Browse directly (renders markdown)

MkDocs site: cd docs-src/mkdocs && mkdocs serve

Sphinx site: cd docs-src/sphinx && make html
