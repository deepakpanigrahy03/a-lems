# Deployment Guide

This guide documents the steps to deploy A-LEMS documentation to GitHub Pages.

---

## 🚀 One-Time Setup

### 1. Create GitHub Actions workflow

File: `.github/workflows/docs.yml`

```yaml
name: Deploy MkDocs Documentation

on:
  push:
    branches: [main]
    paths:
      - 'docs-src/mkdocs/**'
      - 'docs/assets/diagrams/**'

permissions:
  contents: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install mkdocs mkdocs-material pymdown-extensions
      - run: mkdocs build -f docs-src/mkdocs/mkdocs.yml -d site
      - uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./site
          publish_branch: gh-pages
```

### 2. Configure GitHub Pages

1. Go to: `https://github.com/[username]/[repo]/settings/pages`
2. **Source:** Deploy from a branch
3. **Branch:** `gh-pages` → `/ (root)`
4. Click **Save**

---

## 🔧 Manual Deployment (if workflow fails)

```bash
# 1. Build the site (from project root)
cd ~/mydrive/a-lems
mkdocs build -f docs-src/mkdocs/mkdocs.yml -d docs-src/mkdocs/site

# 2. Go to the built site
cd docs-src/mkdocs/site

# 3. Create orphan branch and push
git init
git add .
git commit -m "Deploy MkDocs site"
git push https://github.com/deepakpanigrahy03/a-lems.git HEAD:gh-pages --force

# 4. Return to project root
cd ~/mydrive/a-lems
```

---

## 🔑 GitHub Token Permissions Required

Your Personal Access Token needs:

- ✅ `repo` (full control)
- ✅ `workflow` (to update workflow files)
- ✅ `gist` (optional, for gists)

Update token at: `https://github.com/settings/tokens`

---

## ✅ Verify Deployment

1. Wait 1-2 minutes after push
2. Check Actions tab: `https://github.com/[username]/[repo]/actions`
3. Visit: `https://[username].github.io/[repo]/`

---

## 📚 Troubleshooting

| Issue | Solution |
|-------|----------|
| 404 Not Found | Ensure Pages is set to `gh-pages` branch |
| Empty site | Run `git ls-tree origin/gh-pages` to check contents |
| Workflow fails | Check Actions tab for error logs |
| Token error | Verify `workflow` scope is enabled |

---

*Last updated: March 2026*