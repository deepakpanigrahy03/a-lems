# Minimal makefile for Sphinx documentation
SPHINXBUILD = sphinx-build
SOURCEDIR = source
BUILDDIR = ../../docs/generated/sphinx

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)"

%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)"
