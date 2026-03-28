"""
A-LEMS Report Engine — MkDocs Ingester
Parses mkdocs.yml, loads markdown docs, extracts sections by heading,
resolves image/SVG paths, extracts LaTeX math blocks.
Used by section_builders to inject methodology text into reports.
"""

from __future__ import annotations
import re, logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class DocSection:
    source_file: str        # relative path, e.g. "research/measurement-methodology.md"
    heading: str            # section heading text
    anchor: str             # slug anchor, e.g. "rapl-sensor-collection"
    level: int              # heading level 1-6
    content: str            # plain text content (markdown stripped)
    math_blocks: list[str]  # LaTeX math extracted from $$ ... $$ blocks
    images: list[str]       # resolved absolute paths to images


@dataclass
class KnowledgeGraph:
    sections: list[DocSection]
    images: dict[str, Path]     # anchor/filename → resolved path
    nav_order: list[str]        # doc paths in nav order


def _slug(text: str) -> str:
    """Convert heading text to URL anchor slug."""
    return re.sub(r"[^a-z0-9-]", "", text.lower().replace(" ", "-"))


def _strip_markdown(text: str) -> str:
    """Remove basic markdown syntax, return plain text."""
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)  # code blocks
    text = re.sub(r"`[^`]+`", "", text)                      # inline code
    text = re.sub(r"!\[.*?\]\(.*?\)", "", text)              # images
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)    # links
    text = re.sub(r"[*_]{1,2}([^*_]+)[*_]{1,2}", r"\1", text)  # bold/italic
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)  # headings
    text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE) # bullets
    text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE) # numbered
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_math(text: str) -> list[str]:
    """Extract LaTeX blocks from $$ ... $$ and \[ ... \]."""
    blocks = re.findall(r"\$\$(.+?)\$\$", text, re.DOTALL)
    blocks += re.findall(r"\\\[(.+?)\\\]", text, re.DOTALL)
    return [b.strip() for b in blocks]


def _parse_markdown_file(
    path: Path,
    rel_path: str,
    docs_dir: Path,
) -> list[DocSection]:
    """Parse a markdown file into a list of DocSections (one per heading)."""
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:
        log.warning(f"Cannot read {path}: {e}")
        return []

    sections: list[DocSection] = []
    current_heading = "Introduction"
    current_level = 1
    current_lines: list[str] = []
    current_images: list[str] = []

    def flush():
        if not current_lines:
            return
        content = "\n".join(current_lines)
        # Resolve image paths
        imgs = []
        for m in re.finditer(r"!\[.*?\]\(([^)]+)\)", content):
            img_rel = m.group(1)
            img_path = (path.parent / img_rel).resolve()
            if img_path.exists():
                imgs.append(str(img_path))
        sections.append(DocSection(
            source_file=rel_path,
            heading=current_heading,
            anchor=_slug(current_heading),
            level=current_level,
            content=_strip_markdown(content),
            math_blocks=_extract_math(content),
            images=imgs,
        ))

    for line in raw.splitlines():
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            flush()
            current_lines = []
            current_level = len(m.group(1))
            current_heading = m.group(2).strip()
        else:
            current_lines.append(line)

    flush()
    return sections


def _flatten_nav(nav: Any, prefix: str = "") -> list[str]:
    """Recursively flatten mkdocs nav dict to list of doc paths."""
    paths = []
    if isinstance(nav, list):
        for item in nav:
            paths.extend(_flatten_nav(item, prefix))
    elif isinstance(nav, dict):
        for _, v in nav.items():
            paths.extend(_flatten_nav(v, prefix))
    elif isinstance(nav, str):
        paths.append(nav)
    return paths


class MkDocsIngester:
    """
    Loads and indexes all MkDocs documentation for injection into reports.
    Handles missing docs directory gracefully — returns empty results.
    """

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root)
        self.mkdocs_yml = self.project_root / "mkdocs.yml"
        self.docs_dir = self.project_root / "docs"
        self._kg: KnowledgeGraph | None = None

    def build(self) -> KnowledgeGraph:
        """Parse all docs and build the knowledge graph. Cached after first call."""
        if self._kg is not None:
            return self._kg

        if not self.mkdocs_yml.exists():
            log.warning(f"mkdocs.yml not found at {self.mkdocs_yml}")
            self._kg = KnowledgeGraph(sections=[], images={}, nav_order=[])
            return self._kg

        try:
            import yaml
            with open(self.mkdocs_yml) as f:
                cfg = yaml.safe_load(f)
        except Exception as e:
            log.error(f"Cannot parse mkdocs.yml: {e}")
            self._kg = KnowledgeGraph(sections=[], images={}, nav_order=[])
            return self._kg

        nav = cfg.get("nav", [])
        doc_paths = _flatten_nav(nav)
        all_sections: list[DocSection] = []
        all_images: dict[str, Path] = {}

        for rel_path in doc_paths:
            abs_path = self.docs_dir / rel_path
            if not abs_path.exists():
                log.debug(f"Doc not found: {abs_path}")
                continue
            sections = _parse_markdown_file(abs_path, rel_path, self.docs_dir)
            all_sections.extend(sections)

        # Index SVG/PNG assets
        assets_dir = self.docs_dir / "generated" / "mkdocs" / "assets" / "diagrams"
        if assets_dir.exists():
            for f in assets_dir.iterdir():
                if f.suffix in (".svg", ".png"):
                    all_images[f.stem] = f
                    all_images[f.name] = f

        self._kg = KnowledgeGraph(
            sections=all_sections,
            images=all_images,
            nav_order=doc_paths,
        )
        log.info(
            f"MkDocs ingested: {len(all_sections)} sections, "
            f"{len(all_images)} assets from {len(doc_paths)} docs"
        )
        return self._kg

    def get_section(self, doc_path: str, anchor: str | None = None) -> DocSection | None:
        """
        Retrieve a specific section.
        doc_path: 'research/measurement-methodology.md'
        anchor:   'rapl-sensor-collection' (optional — returns first section if None)
        """
        kg = self.build()
        for s in kg.sections:
            if s.source_file == doc_path:
                if anchor is None or s.anchor == anchor:
                    return s
        return None

    def get_sections_for_doc(self, doc_path: str) -> list[DocSection]:
        kg = self.build()
        return [s for s in kg.sections if s.source_file == doc_path]

    def get_diagram_path(self, diagram_id: str) -> Path | None:
        """
        Resolve a diagram ID to an absolute file path.
        diagram_id can be 'architecture', 'architecture.svg', etc.
        """
        kg = self.build()
        path = kg.images.get(diagram_id) or kg.images.get(diagram_id + ".svg")
        if path and path.exists():
            return path
        # Fallback: runtime-generated diagram placeholder
        log.debug(f"Diagram not found on disk: {diagram_id} — will generate at runtime")
        return None

    def parse_doc_key(self, key: str) -> tuple[str, str | None]:
        """
        Parse 'research/measurement-methodology.md#rapl-sensor-collection'
        into (doc_path, anchor_or_None).
        """
        if "#" in key:
            doc, anchor = key.split("#", 1)
            return doc.strip(), anchor.strip()
        return key.strip(), None

    def resolve_doc_sections(self, keys: list[str]) -> list[DocSection]:
        """
        Given a list of doc section keys (from goal YAML), return
        the corresponding DocSection objects.
        """
        results = []
        for key in keys:
            doc_path, anchor = self.parse_doc_key(key)
            if anchor:
                section = self.get_section(doc_path, anchor)
                if section:
                    results.append(section)
            else:
                results.extend(self.get_sections_for_doc(doc_path))
        return results
