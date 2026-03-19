#!/bin/bash
# Build all documentation

echo "📚 Building A-LEMS Documentation"
echo "================================"

# Build MkDocs
echo "📖 Building MkDocs site..."
if [ -f "docs-src/mkdocs/mkdocs.yml" ]; then
    cd docs-src/mkdocs
    mkdocs build --site-dir ../../docs/generated/mkdocs
    cd ../..
    echo "   ✅ MkDocs built"
else
    echo "   ⚠️ MkDocs config not found"
fi

# Build Sphinx
echo "📖 Building Sphinx docs..."
if [ -f "docs-src/sphinx/source/conf.py" ]; then
    cd docs-src/sphinx
    sphinx-build -b html source/ ../../docs/generated/sphinx/
    cd ../..
    echo "   ✅ Sphinx built"
else
    echo "   ⚠️ Sphinx config not found"
fi

echo "✅ Documentation build complete!"
echo "   📁 Generated files in docs/generated/"
