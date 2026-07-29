#!/usr/bin/env python3
"""Quality gate for the spec suite in spec/.

Checks the things a human reviewer cannot hold in their head: that every ID
reference resolves, that nothing defined is dead, that the traceability
matrices in 13 cover what 02/07/08/09/11 define, that the ADR log and the
parameter registry agree with reality, and that links point at files and
headings that exist.

Run `tools/spec-lint.py --list-checks` for the catalog.
Exit status: 1 if any error-severity finding survives the ignore file.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Suite conventions. Edit here when the spec's shape changes.
# ---------------------------------------------------------------------------

# prefix -> (home document, regex matching a definition line, kind)
# kind "strong": `**ID — Title.**` or `## ID — Title`; one definition per ID.
# kind "row":    `| ID | ...` first cell of a table; the same ID may legitimately
#                reappear as a row in a *different* section (e.g. SLI in the SLO
#                table), so duplicates are judged per section.
DEFINITIONS: dict[str, tuple[str, str, str]] = {
    "REQ": ("02-requirements.md", r"^\*\*REQ-\d+ — ", "strong"),
    "OQ":  ("02-requirements.md", r"^\| OQ-\d+ \|", "row"),
    "DEF": ("03-data-model.md",   r"^\*\*DEF-\d+ — ", "strong"),
    "OP":  ("04-operations.md",   r"^## OP-\d+ — ", "strong"),
    "DC":  ("05-dependencies.md", r"^## DC-\d+ — ", "strong"),
    "INV": ("07-invariants.md",   r"^\*\*INV-\d+ — ", "strong"),
    "LIV": ("08-liveness.md",     r"^\*\*LIV-\d+ — ", "strong"),
    "FM":  ("09-failure-model.md", r"^\*\*FM-\d+ — ", "strong"),
    "PF":  ("11-performance.md",  r"^\*\*PF-\d+ — ", "strong"),
    "SLI": ("11-performance.md",  r"^\| SLI-\d+ \|", "row"),
    "HZ":  ("11-performance.md",  r"^\| HZ-\d+ \|", "row"),
    "OB":  ("12-observability.md", r"^\*\*OB-\d+ — ", "strong"),
    "CT":  ("13-conformance.md",  r"^\| CT-\d+ \|", "row"),
    "FV":  ("13-conformance.md",  r"^\| FV-\d+ \|", "row"),
    "GAP": ("13-conformance.md",  r"^\| GAP-\d+ \|", "row"),
    "IB":  ("14-interface-binding.md", r"^\*\*IB-\d+ — ", "strong"),
}

# An ID may also be *retired* rather than defined: 13 closes gaps and 02 closes
# open questions in prose, as a bolded ID. Those still count as defined.
CLOSED_FORM = {"GAP": r"^-\s+\*\*GAP-(\d+)", "OQ": r"\*\*OQ-(\d+)\*\*"}

PARAM_HOME = "15-parameters.md"
PARAM_ROW = re.compile(r"^\|\s*(P-[A-Z0-9][A-Z0-9-]*)\s*\|")
PARAM_REF = re.compile(r"\bP-[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+\b")

CONFORMANCE = "13-conformance.md"
REQUIREMENTS = "02-requirements.md"
README = "README.md"

# Which prefixes must appear in which matrix of 13.
MATRIX_PROPERTIES = ("INV", "LIV", "FM", "SLI")
MATRIX_REQUIREMENTS = ("REQ",)

# Docs whose normative text must not carry bare dimensioned constants (the
# `P-*`-only rule). 13 and 15 are the mutable registries; ADRs are exempt.
NORMATIVE_DOCS = (
    "01-overview.md", "02-requirements.md", "03-data-model.md",
    "04-operations.md", "05-dependencies.md", "07-invariants.md",
    "08-liveness.md", "09-failure-model.md", "11-performance.md",
    "12-observability.md", "14-interface-binding.md",
)
# Sections of those documents that are explicitly not normative text.
NON_NORMATIVE_SECTIONS = ("Open questions", "Explicitly unspecified", "Glossary")
# The one normative doc allowed to name the concrete surface (routes, headers, codes).
BINDING_DOC = "14-interface-binding.md"
BARE_CONSTANT = re.compile(
    r"(?<![\w.-])\d+(?:\.\d+)?\s?(?:ms|s|min|h|KiB|MiB|GiB|TiB|KB|MB|GB|TB)\b"
)

# The tag may carry a qualifier: `[MUST — intent, currently violated]`.
RFC2119 = re.compile(r"\[(MUST|SHOULD|MAY)(\s+NOT)?\b[^\]]*\]")
INV_SCOPE = re.compile(r"\[(state|transition|response|recovery)\]")
ADR_STATUSES = {"Accepted", "Proposed", "Superseded", "Rejected", "Deprecated"}

# Prefixes whose home document declares numbering bands ("Bands: 1–9 … 20–29 …").
# A hole that straddles a band boundary is the convention working as intended; a
# hole inside one band is an ID that was removed instead of retired.
BANDED = {"REQ", "INV"}
BAND_LINE = re.compile(r"Bands?:")
BAND_RANGE = re.compile(r"(\d+)\s*[–-]\s*(\d+)")

# A ⚠ marker must lead somewhere: a decision, an open question, a gap, or a
# parameter row that itself names the ratification path.
RATIFICATION = re.compile(r"\b(ADR-\d+|OQ-\d+|GAP-\d+|ratif\w*|draft|proposed)\b", re.I)

CHECKS: dict[str, tuple[str, str]] = {
    # name: (severity, description)
    "link-missing-file":      ("error", "relative link points at a file that does not exist"),
    "link-missing-anchor":    ("error", "link fragment names no heading in the target file"),
    "id-undefined":           ("error", "ID is referenced but never defined in its home document"),
    "id-duplicate":           ("error", "ID is defined more than once"),
    "id-orphan":              ("warn",  "ID is defined but never referenced anywhere else"),
    "id-number-gap":          ("info",  "unused number inside an ID band (canonical, but confirm intent)"),
    "param-undefined":        ("error", "P-* symbol used with no row in the parameter registry"),
    "param-unused":           ("warn",  "parameter registry row that nothing references"),
    "param-bare-constant":    ("warn",  "dimensioned literal in normative text; use a P-* symbol"),
    "impl-leakage":           ("warn",  "implementation name in normative text (crate, module, path, type)"),
    "adr-missing-file":       ("error", "decision log row has no ADR file"),
    "adr-missing-row":        ("error", "ADR file has no row in the decision log"),
    "adr-undefined":          ("error", "ADR-n referenced but neither file nor log row exists"),
    "adr-status-mismatch":    ("error", "ADR status disagrees between the file and the decision log"),
    "adr-date-mismatch":      ("error", "ADR date disagrees between the file and the decision log"),
    "adr-bad-status":         ("error", "ADR file has no parseable `Status:` line"),
    "adr-id-mismatch":        ("error", "ADR filename, heading, or log row disagree on the number"),
    "adr-title-drift":        ("info",  "decision-log title differs from the ADR heading"),
    "adr-log-order":          ("warn",  "accepted decisions are not in ascending number/date order"),
    "trace-missing":          ("error", "defined property/requirement has no row in a 13 matrix"),
    "trace-unknown":          ("error", "13 matrix row names an ID that is not defined"),
    "doc-unlisted":           ("error", "spec document missing from the README document map"),
    "doc-missing":            ("error", "README document map lists a file that does not exist"),
    "req-missing-tag":        ("error", "requirement carries no RFC 2119 keyword"),
    "req-missing-acceptance": ("error", "requirement has no *Acceptance:* clause"),
    "inv-missing-scope":      ("error", "invariant carries no scope tag"),
    "inv-missing-check":      ("error", "invariant names no check strategy"),
    "inv-check-unknown":      ("error", "invariant's *Check:* names an undefined test class"),
    "heading-duplicate":      ("warn",  "duplicate heading text; anchors become ambiguous"),
    "stale-matrix":           ("warn",  "normative doc changed after the conformance doc's stated date"),
    "stale-baseline":         ("warn",  "conformance doc pins a commit that is not in this history"),
    "date-inconsistent":      ("warn",  "a mutable doc states two different as-of dates"),
    "warn-unratified":        ("warn",  "⚠ marker with no route to ratification"),
}

ID_TOKEN = re.compile(
    r"\b(" + "|".join(sorted(DEFINITIONS, key=len, reverse=True)) + r")-(\d+)"
    r"((?:\s*(?:/|\.\.)\s*(?:(?:REQ|OQ|DEF|OP|DC|INV|LIV|FM|PF|SLI|HZ|OB|CT|FV|GAP|IB)-)?\d+)*)"
)
CONTINUATION = re.compile(r"(/|\.\.)\s*(?:[A-Z]+-)?(\d+)")
ADR_REF = re.compile(r"\bADR-(\d+)")
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
FENCE = re.compile(r"^\s*(```|~~~)")


# ---------------------------------------------------------------------------


@dataclass
class Finding:
    check: str
    file: str
    line: int
    message: str

    @property
    def severity(self) -> str:
        return CHECKS[self.check][0]


@dataclass
class Doc:
    path: Path
    rel: str
    lines: list[str]
    # line number -> True when the line sits inside a fenced code block
    fenced: list[bool] = field(default_factory=list)
    headings: list[tuple[int, int, str]] = field(default_factory=list)  # (line, level, text)

    def section_at(self, line: int) -> str:
        current = ""
        for ln, level, text in self.headings:
            if ln > line:
                break
            if level == 2:
                current = text
        return current


def slugify(text: str) -> str:
    """GitHub's heading-anchor algorithm, near enough for internal links."""
    text = re.sub(r"[`*_~]", "", text).strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    # One hyphen per space, not per run: `A — B` loses the dash and keeps both
    # spaces, so GitHub's anchor is `a--b`. Collapsing here rejects the real
    # anchor and accepts the broken one.
    return re.sub(r"\s", "-", text)


def load(spec_dir: Path) -> list[Doc]:
    docs = []
    paths = sorted(spec_dir.glob("*.md")) + sorted((spec_dir / "decisions").glob("*.md"))
    for path in paths:
        lines = path.read_text(encoding="utf-8").splitlines()
        doc = Doc(path=path, rel=str(path.relative_to(spec_dir)), lines=lines)
        inside = False
        for raw in lines:
            if FENCE.match(raw):
                inside = not inside
                doc.fenced.append(True)
                continue
            doc.fenced.append(inside)
        for i, raw in enumerate(lines, 1):
            if doc.fenced[i - 1]:
                continue
            m = re.match(r"^(#{1,6})\s+(.*?)\s*$", raw)
            if m:
                doc.headings.append((i, len(m.group(1)), m.group(2)))
        docs.append(doc)
    return docs


def expand_ids(match: re.Match) -> tuple[list[str], list[str]]:
    """Return (hard, soft) IDs for one reference token.

    `INV-20/21/25` names three IDs outright — all hard. `CT-2..CT-9` names its
    endpoints outright but the interior is a shorthand that may legitimately
    span a band gap, so interior members are soft: they satisfy the orphan
    check without being able to raise `id-undefined`.
    """
    prefix, first, tail = match.group(1), int(match.group(2)), match.group(3) or ""
    hard = [f"{prefix}-{first}"]
    soft: list[str] = []
    cursor = first
    for op, num in CONTINUATION.findall(tail):
        num = int(num)
        if op == "/":
            hard.append(f"{prefix}-{num}")
        else:
            hard.append(f"{prefix}-{num}")
            soft.extend(f"{prefix}-{n}" for n in range(cursor + 1, num))
        cursor = num
    return hard, soft


def parse_table_rows(doc: Doc, start: int, end: int) -> list[tuple[int, list[str]]]:
    rows = []
    for i in range(start, min(end, len(doc.lines))):
        raw = doc.lines[i]
        if doc.fenced[i] or not raw.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in raw.strip().strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):
            continue
        rows.append((i + 1, cells))
    return rows


def section_bounds(doc: Doc, title_match) -> tuple[int, int] | None:
    for idx, (ln, level, text) in enumerate(doc.headings):
        if level == 2 and title_match(text):
            end = len(doc.lines)
            for ln2, level2, _ in doc.headings[idx + 1:]:
                if level2 <= 2:
                    end = ln2 - 1
                    break
            return ln, end
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


class Linter:
    def __init__(self, spec_dir: Path, repo_root: Path):
        self.spec_dir = spec_dir
        self.repo_root = repo_root
        self.docs = load(spec_dir)
        self.by_rel = {d.rel: d for d in self.docs}
        self.findings: list[Finding] = []
        self.definitions: dict[str, tuple[str, int]] = {}
        self.closed: set[str] = set()
        self.references: dict[str, list[tuple[str, int]]] = defaultdict(list)
        self.soft_references: set[str] = set()
        self.params: dict[str, int] = {}
        self.param_refs: dict[str, list[tuple[str, int]]] = defaultdict(list)
        self.adr_files: dict[int, Doc] = {}
        self.adr_rows: dict[int, dict] = {}

    def report(self, check: str, doc_rel: str, line: int, message: str) -> None:
        self.findings.append(Finding(check, doc_rel, line, message))

    # -- collection --------------------------------------------------------

    def collect(self) -> None:
        for doc in self.docs:
            if doc.rel.startswith("decisions/"):
                m = re.match(r"decisions/ADR-(\d+)-", doc.rel)
                if m:
                    self.adr_files[int(m.group(1))] = doc

            for i, raw in enumerate(doc.lines, 1):
                fenced = doc.fenced[i - 1]

                if not fenced:
                    self._collect_definitions(doc, i, raw)
                    if doc.rel == PARAM_HOME:
                        pm = PARAM_ROW.match(raw)
                        if pm:
                            self.params.setdefault(pm.group(1), i)

                for m in ID_TOKEN.finditer(raw):
                    hard, soft = expand_ids(m)
                    for ident in hard:
                        self.references[ident].append((doc.rel, i))
                    self.soft_references.update(soft)
                for m in PARAM_REF.finditer(raw):
                    self.param_refs[m.group(0)].append((doc.rel, i))

    def _collect_definitions(self, doc: Doc, line: int, raw: str) -> None:
        for prefix, (home, pattern, kind) in DEFINITIONS.items():
            if doc.rel != home:
                continue
            if re.match(pattern, raw):
                # A strong definition line may introduce several IDs
                # (`## OP-6 — … · OP-7 — …`); a row defines exactly its first cell.
                if kind == "strong":
                    idents = [f"{prefix}-{n}" for n in re.findall(rf"{prefix}-(\d+) — ", raw)]
                else:
                    idents = [f"{prefix}-{re.match(pattern, raw).group(0).split('-')[1].strip(' |')}"]
                for ident in idents:
                    self._define(doc, line, ident, kind)
            closed = CLOSED_FORM.get(prefix)
            if closed:
                cm = re.search(closed, raw)
                if cm:
                    self._define(doc, line, f"{prefix}-{cm.group(1)}", "closed")

    def _define(self, doc: Doc, line: int, ident: str, kind: str) -> None:
        if kind == "closed":
            self.closed.add(ident)
        prior = self.definitions.get(ident)
        if prior is None:
            self.definitions[ident] = (doc.rel, line)
            return
        if kind == "closed" or prior[0] != doc.rel:
            return
        # Row-form IDs may reappear in a different table of the same document
        # (SLI rows recur in the SLO table); only a repeat within one section
        # is a genuine double definition.
        if kind == "row" and doc.section_at(prior[1]) != doc.section_at(line):
            return
        self.report("id-duplicate", doc.rel, line,
                    f"{ident} is already defined at {prior[0]}:{prior[1]}")

    # -- individual checks -------------------------------------------------

    def check_links(self) -> None:
        for doc in self.docs:
            for i, raw in enumerate(doc.lines, 1):
                if doc.fenced[i - 1]:
                    continue
                for m in LINK.finditer(raw):
                    target = m.group(1)
                    if re.match(r"^(https?:|mailto:|#)", target):
                        if target.startswith("#"):
                            self._check_anchor(doc, i, doc, target[1:], target)
                        continue
                    path_part, _, frag = target.partition("#")
                    resolved = (doc.path.parent / path_part).resolve()
                    if not resolved.exists():
                        self.report("link-missing-file", doc.rel, i,
                                    f"link target `{target}` does not exist")
                        continue
                    if frag:
                        try:
                            rel = str(resolved.relative_to(self.spec_dir))
                        except ValueError:
                            continue
                        other = self.by_rel.get(rel)
                        if other:
                            self._check_anchor(doc, i, other, frag, target)

    def _check_anchor(self, doc: Doc, line: int, target_doc: Doc, frag: str, shown: str) -> None:
        slugs = {slugify(text) for _, _, text in target_doc.headings}
        if frag.lower() not in slugs:
            self.report("link-missing-anchor", doc.rel, line,
                        f"`{shown}` names no heading in {target_doc.rel}")

    def check_ids(self) -> None:
        for ident, sites in sorted(self.references.items()):
            if ident in self.definitions:
                continue
            prefix = ident.split("-")[0]
            home = DEFINITIONS[prefix][0]
            doc_rel, line = sites[0]
            others = f" (+{len(sites) - 1} more)" if len(sites) > 1 else ""
            self.report("id-undefined", doc_rel, line,
                        f"{ident} is referenced but not defined in {home}{others}")

        for ident, (doc_rel, line) in sorted(self.definitions.items()):
            if ident in self.closed:
                continue  # retired IDs are kept so numbers are never recycled
            elsewhere = [s for s in self.references.get(ident, []) if s != (doc_rel, line)]
            if not elsewhere and ident not in self.soft_references:
                self.report("id-orphan", doc_rel, line,
                            f"{ident} is defined but never referenced elsewhere")

        by_prefix: dict[str, list[int]] = defaultdict(list)
        for ident in self.definitions:
            prefix, _, num = ident.rpartition("-")
            by_prefix[prefix].append(int(num))
        for prefix, nums in sorted(by_prefix.items()):
            nums.sort()
            home = DEFINITIONS[prefix][0]
            bands = self._bands(prefix)
            for a, b in zip(nums, nums[1:]):
                if b - a == 1:
                    continue
                if bands and self._band_of(a, bands) != self._band_of(b, bands):
                    continue  # the gap is the band boundary the doc declares
                missing = ", ".join(f"{prefix}-{n}" for n in range(a + 1, b))
                self.report("id-number-gap", home, self.definitions[f"{prefix}-{b}"][1],
                            f"{missing} unused between {prefix}-{a} and {prefix}-{b}")

    def _bands(self, prefix: str) -> list[tuple[int, int]]:
        if prefix not in BANDED:
            return []
        doc = self.by_rel.get(DEFINITIONS[prefix][0])
        if not doc:
            return []
        for i, raw in enumerate(doc.lines[:20]):
            if not BAND_LINE.search(raw):
                continue
            paragraph = raw
            for nxt in doc.lines[i + 1:i + 5]:
                if not nxt.strip():
                    break
                paragraph += " " + nxt
            return [(int(a), int(b)) for a, b in BAND_RANGE.findall(paragraph)]
        return []

    @staticmethod
    def _band_of(num: int, bands: list[tuple[int, int]]) -> int | None:
        for i, (lo, hi) in enumerate(bands):
            if lo <= num <= hi:
                return i
        return None

    def check_params(self) -> None:
        for name, sites in sorted(self.param_refs.items()):
            if name in self.params:
                continue
            doc_rel, line = sites[0]
            self.report("param-undefined", doc_rel, line,
                        f"{name} has no row in {PARAM_HOME}")
        for name, line in sorted(self.params.items()):
            used = [s for s in self.param_refs.get(name, []) if s[0] != PARAM_HOME]
            if not used:
                self.report("param-unused", PARAM_HOME, line,
                            f"{name} is registered but referenced by no other document")

    def check_bare_constants(self) -> None:
        for rel in NORMATIVE_DOCS:
            doc = self.by_rel.get(rel)
            if not doc:
                continue
            for i, raw in enumerate(doc.lines, 1):
                if doc.fenced[i - 1]:
                    continue
                if doc.section_at(i).startswith(NON_NORMATIVE_SECTIONS):
                    continue
                for m in BARE_CONSTANT.finditer(raw):
                    self.report("param-bare-constant", rel, i,
                                f"literal `{m.group(0)}` in normative text; "
                                f"normative constants live in {PARAM_HOME} as P-* symbols")

    def check_impl_leakage(self) -> None:
        """Normative docs name mechanisms, never the code implementing them.

        Only `code spans` are examined, and only two shapes count: a language path
        (`Type::method`) and a span that *is* a manifest dependency. Matching source
        module names instead would flag the wire surface — modules here are named for
        the domain nouns the routes already use — and matching prose would flag every
        sentence sharing a word with a crate.
        """
        deps: set[str] = set()
        manifest = self.repo_root / "Cargo.toml"
        if manifest.exists():
            in_deps = False
            for raw in manifest.read_text(encoding="utf-8").splitlines():
                if raw.startswith("["):
                    in_deps = "dependencies" in raw
                    continue
                m = re.match(r"^([A-Za-z][\w-]{2,})\s*=", raw) if in_deps else None
                if m:
                    deps.add(m.group(1).lower())

        token = re.compile(r"`([^`]+)`")
        for rel in NORMATIVE_DOCS:
            if rel == BINDING_DOC:
                continue  # the one doc whose job is naming the concrete surface
            doc = self.by_rel.get(rel)
            if not doc:
                continue
            for i, raw in enumerate(doc.lines, 1):
                if doc.fenced[i - 1]:
                    continue
                for m in token.finditer(raw):
                    span = m.group(1).strip()
                    if "::" in span:
                        why = "a language path"
                    elif span.lower() in deps:
                        why = "a dependency of this project"
                    else:
                        continue
                    self.report("impl-leakage", rel, i,
                                f"`{span}` is {why}; normative text describes mechanisms, "
                                f"not code (14 and ADRs are exempt)")

    def check_document_map(self) -> None:
        readme = self.by_rel.get(README)
        if not readme:
            return
        listed = set()
        for i, raw in enumerate(readme.lines, 1):
            for m in LINK.finditer(raw):
                target = m.group(1).partition("#")[0]
                if target.endswith(".md") and not target.startswith("http"):
                    listed.add(target)
                    if not (readme.path.parent / target).exists():
                        self.report("doc-missing", README, i,
                                    f"document map links `{target}`, which does not exist")
        for doc in self.docs:
            if doc.rel in (README,) or doc.rel.startswith("decisions/"):
                continue
            if doc.rel not in listed:
                self.report("doc-unlisted", README, 1,
                            f"{doc.rel} exists but is not linked from the document map")

    def check_adrs(self) -> None:
        readme = self.by_rel.get(README)
        if readme:
            for line, cells in parse_table_rows(readme, 0, len(readme.lines)):
                if not cells or not re.match(r"^ADR-\d+$", cells[0]):
                    continue
                num = int(cells[0].split("-")[1])
                link = LINK.search(cells[2]) if len(cells) > 2 else None
                plain = lambda s: re.sub(r"[*_`]", "", s).strip()
                self.adr_rows[num] = {
                    "line": line,
                    "date": plain(cells[1]) if len(cells) > 1 else "",
                    "title": plain(re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", cells[2])) if len(cells) > 2 else "",
                    "status": plain(cells[3]) if len(cells) > 3 else "",
                    "link": link.group(1) if link else None,
                }

        for num, row in sorted(self.adr_rows.items()):
            if num in self.adr_files:
                continue
            if row["link"] is None:
                continue  # externally-owned decision; the row explains itself
            self.report("adr-missing-file", README, row["line"],
                        f"ADR-{num:03d} is logged but decisions/ has no file for it")

        for num, doc in sorted(self.adr_files.items()):
            row = self.adr_rows.get(num)
            if row is None:
                self.report("adr-missing-row", doc.rel, 1,
                            f"ADR-{num:03d} exists but has no row in the {README} decision log")

            heading = doc.lines[0] if doc.lines else ""
            hm = re.match(r"^# ADR-(\d+) — (.*)$", heading)
            if not hm:
                self.report("adr-id-mismatch", doc.rel, 1,
                            "first line is not `# ADR-nnn — Title`")
                continue
            if int(hm.group(1)) != num:
                self.report("adr-id-mismatch", doc.rel, 1,
                            f"heading says ADR-{hm.group(1)} but the filename says ADR-{num:03d}")

            status_line = next(((i, l) for i, l in enumerate(doc.lines[:12], 1)
                                if l.startswith("Status:")), None)
            if status_line is None:
                self.report("adr-bad-status", doc.rel, 1, "no `Status:` line in the first 12 lines")
                continue
            sline, stext = status_line
            sm = re.match(r"^Status:\s+(\w+)", stext)
            if not sm or sm.group(1) not in ADR_STATUSES:
                self.report("adr-bad-status", doc.rel, sline,
                            f"unrecognized status; expected one of {sorted(ADR_STATUSES)}")
                continue
            status = sm.group(1)
            date = re.search(r"\d{4}-\d{2}-\d{2}", stext)

            if row is None:
                continue
            if not row["status"].startswith(status):
                self.report("adr-status-mismatch", README, row["line"],
                            f"log says `{row['status']}`, {doc.rel} says `{status}`")
            if date and row["date"] and date.group(0) != row["date"]:
                self.report("adr-date-mismatch", README, row["line"],
                            f"log dates ADR-{num:03d} {row['date']}, the file says {date.group(0)}")
            norm = lambda s: re.sub(r"\s+", " ", re.sub(r"\([^)]*\)", "", s)).strip().lower()
            if norm(row["title"]) != norm(hm.group(2)):
                self.report("adr-title-drift", README, row["line"],
                            f"log title differs from the ADR heading: "
                            f"`{row['title'].strip()}` vs `{hm.group(2)}`")

        known = set(self.adr_files) | set(self.adr_rows)
        seen: dict[int, tuple[str, int]] = {}
        for doc in self.docs:
            for i, raw in enumerate(doc.lines, 1):
                for m in ADR_REF.finditer(raw):
                    seen.setdefault(int(m.group(1)), (doc.rel, i))
        for num, (rel, line) in sorted(seen.items()):
            if num not in known:
                self.report("adr-undefined", rel, line,
                            f"ADR-{num:03d} is referenced but neither logged nor filed")

        accepted = [(n, r) for n, r in sorted(self.adr_rows.items())
                    if r["status"].startswith("Accepted")]
        rows_in_order = sorted(accepted, key=lambda kv: kv[1]["line"])
        for (n1, r1), (n2, r2) in zip(rows_in_order, rows_in_order[1:]):
            if n2 < n1 or (r1["date"] and r2["date"] and r2["date"] < r1["date"]):
                self.report("adr-log-order", README, r2["line"],
                            f"ADR-{n2:03d} ({r2['date']}) is logged after "
                            f"ADR-{n1:03d} ({r1['date']}); accepted decisions run in date order")

    def check_traceability(self) -> None:
        doc = self.by_rel.get(CONFORMANCE)
        if not doc:
            return
        matrices = {
            "properties": (MATRIX_PROPERTIES, section_bounds(doc, lambda t: t.startswith("Traceability matrix"))),
            "requirements": (MATRIX_REQUIREMENTS, section_bounds(doc, lambda t: t.startswith("Acceptance matrix"))),
        }
        for label, (prefixes, bounds) in matrices.items():
            if bounds is None:
                self.report("trace-missing", CONFORMANCE, 1,
                            f"no {label} matrix section found")
                continue
            start, end = bounds
            covered: set[str] = set()
            for line, cells in parse_table_rows(doc, start, end):
                if not cells:
                    continue
                for m in ID_TOKEN.finditer(cells[0]):
                    hard, soft = expand_ids(m)
                    for ident in hard + soft:
                        covered.add(ident)
                        if ident not in self.definitions and ident not in soft:
                            self.report("trace-unknown", CONFORMANCE, line,
                                        f"{label} matrix row names {ident}, which is not defined")
            for ident, (home, dline) in sorted(self.definitions.items()):
                if ident.split("-")[0] in prefixes and ident not in covered:
                    self.report("trace-missing", home, dline,
                                f"{ident} has no row in the {label} matrix of {CONFORMANCE}")

    def check_normative_shape(self) -> None:
        req = self.by_rel.get(REQUIREMENTS)
        if req:
            for i, raw in enumerate(req.lines, 1):
                if req.fenced[i - 1] or not re.match(r"^\*\*REQ-\d+ — ", raw):
                    continue
                ident = re.match(r"^\*\*(REQ-\d+)", raw).group(1)
                block = self._block(req, i)
                if not RFC2119.search("\n".join(block)):
                    self.report("req-missing-tag", REQUIREMENTS, i,
                                f"{ident} carries no [MUST]/[SHOULD]/[MAY] tag")
                if not any(l.startswith("*Acceptance:*") for l in block):
                    self.report("req-missing-acceptance", REQUIREMENTS, i,
                                f"{ident} has no *Acceptance:* clause")

        inv = self.by_rel.get(DEFINITIONS["INV"][0])
        if inv:
            for i, raw in enumerate(inv.lines, 1):
                if inv.fenced[i - 1] or not re.match(r"^\*\*INV-\d+ — ", raw):
                    continue
                ident = re.match(r"^\*\*(INV-\d+)", raw).group(1)
                block = self._block(inv, i)
                text = "\n".join(block)
                if not INV_SCOPE.search(text):
                    self.report("inv-missing-scope", inv.rel, i,
                                f"{ident} carries no [state]/[transition]/[response]/[recovery] tag")
                check = [l for l in block if l.startswith("*Check:*")]
                if not check:
                    self.report("inv-missing-check", inv.rel, i,
                                f"{ident} names no *Check:* strategy")
                    continue
                classes = re.findall(r"\bCT-(\d+)", " ".join(check))
                if not classes:
                    self.report("inv-check-unknown", inv.rel, i,
                                f"{ident}'s *Check:* names no CT class")
                for n in classes:
                    if f"CT-{n}" not in self.definitions:
                        self.report("inv-check-unknown", inv.rel, i,
                                    f"{ident} checks against CT-{n}, which {CONFORMANCE} does not define")

    @staticmethod
    def _block(doc: Doc, start_line: int) -> list[str]:
        block = []
        for raw in doc.lines[start_line - 1:]:
            if block and (raw.startswith("**") or raw.startswith("#")):
                break
            block.append(raw)
        return block

    def check_headings(self) -> None:
        for doc in self.docs:
            seen: dict[str, int] = {}
            for line, _, text in doc.headings:
                slug = slugify(text)
                if slug in seen:
                    self.report("heading-duplicate", doc.rel, line,
                                f"heading `{text}` repeats {doc.rel}:{seen[slug]}; the anchor is ambiguous")
                seen[slug] = line

    def check_unratified_markers(self) -> None:
        registry = self.by_rel.get(PARAM_HOME)
        resolved: set[str] = set()
        if registry:
            for name, line in self.params.items():
                if RATIFICATION.search(registry.lines[line - 1]):
                    resolved.add(name)

        for doc in self.docs:
            # README defines the ⚠ convention; ADRs are the ratification path.
            if doc.rel.startswith("decisions/") or doc.rel == README:
                continue
            for i, raw in enumerate(doc.lines, 1):
                if "⚠" not in raw or doc.fenced[i - 1]:
                    continue
                if raw.lstrip().startswith("|"):
                    nxt = doc.lines[i] if i < len(doc.lines) else ""
                    if set(nxt.strip().strip("|")) <= set("-: |"):
                        continue  # column header; the rows carry the routes
                    scope = raw
                else:
                    scope = " ".join(self._paragraph(doc, i))
                if RATIFICATION.search(scope):
                    continue
                # Deferring to a parameter whose registry row carries the
                # ratification path is the documented indirection.
                if any(m.group(0) in resolved for m in PARAM_REF.finditer(scope)):
                    continue
                self.report("warn-unratified", doc.rel, i,
                            "⚠ marker with no ADR/OQ/GAP/parameter route to ratification")

    @staticmethod
    def _paragraph(doc: Doc, line: int) -> list[str]:
        start = line - 1
        while start > 0 and doc.lines[start - 1].strip():
            start -= 1
        end = line
        while end < len(doc.lines) and doc.lines[end].strip():
            end += 1
        return doc.lines[start:end]

    def check_freshness(self) -> None:
        doc = self.by_rel.get(CONFORMANCE)
        if not doc:
            return
        header = "\n".join(doc.lines[:12])
        stated = re.search(r"Statuses as of \*\*(\d{4}-\d{2}-\d{2})\*\*", header)
        sha = re.search(r"master@([0-9a-f]{7,40})", header)

        if sha and not self._git_has(sha.group(1)):
            self.report("stale-baseline", CONFORMANCE, 3,
                        f"pinned baseline commit {sha.group(1)[:12]} is not in this repository's history")

        if not stated:
            return
        # Section headings may carry their own as-of dates ("Gap register — …").
        # They must agree with the document's, and the newest is the real bar.
        dated_sections = [(ln, text, m.group(1)) for ln, level, text in doc.headings
                          if (m := re.search(r"—\s*(\d{4}-\d{2}-\d{2})", text))]
        for ln, text, when in dated_sections:
            if when != stated.group(1):
                self.report("date-inconsistent", CONFORMANCE, ln,
                            f"`{text}` is dated {when} but the document header says "
                            f"{stated.group(1)}")
        bar = max([stated.group(1)] + [d for _, _, d in dated_sections])

        for rel in NORMATIVE_DOCS:
            when = self._git_last_change(self.spec_dir / rel)
            if when and when > bar:
                self.report("stale-matrix", rel, 1,
                            f"last changed {when}, after {CONFORMANCE}'s newest stated date "
                            f"{bar}; its matrices and statuses may not reflect this document")
        dirty = self._git_dirty()
        for rel in NORMATIVE_DOCS:
            if f"spec/{rel}" in dirty and f"spec/{CONFORMANCE}" not in dirty:
                self.report("stale-matrix", rel, 1,
                            f"modified in the working tree while {CONFORMANCE} is not; "
                            "matrices and statuses update in the same change")

    def _git(self, *args: str) -> str | None:
        try:
            out = subprocess.run(("git", "-C", str(self.repo_root)) + args,
                                 capture_output=True, text=True, timeout=15)
        except (OSError, subprocess.SubprocessError):
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    def _git_last_change(self, path: Path) -> str | None:
        return self._git("log", "-1", "--format=%cs", "--", str(path)) or None

    def _git_has(self, sha: str) -> bool:
        return self._git("cat-file", "-e", f"{sha}^{{commit}}") is not None

    def _git_dirty(self) -> set[str]:
        out = self._git("status", "--porcelain")
        if not out:
            return set()
        return {line[3:].strip() for line in out.splitlines()}

    def run(self) -> list[Finding]:
        self.collect()
        self.check_links()
        self.check_ids()
        self.check_params()
        self.check_bare_constants()
        self.check_impl_leakage()
        self.check_document_map()
        self.check_adrs()
        self.check_traceability()
        self.check_normative_shape()
        self.check_headings()
        self.check_unratified_markers()
        self.check_freshness()
        order = {"error": 0, "warn": 1, "info": 2}
        self.findings.sort(key=lambda f: (order[f.severity], f.check, f.file, f.line))
        return self.findings


# ---------------------------------------------------------------------------


def load_ignores(path: Path) -> list[tuple[str, str, re.Pattern]]:
    """`check | file-glob | message-regex` per line; `#` comments."""
    rules = []
    if not path.exists():
        return rules
    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.split("#", 1)[0].strip()
        if not raw:
            continue
        parts = [p.strip() for p in raw.split("|")]
        check = parts[0]
        glob = parts[1] if len(parts) > 1 and parts[1] else "*"
        pattern = re.compile(parts[2]) if len(parts) > 2 and parts[2] else re.compile("")
        rules.append((check, glob, pattern))
    return rules


def ignored(finding: Finding, rules) -> bool:
    from fnmatch import fnmatch
    return any(finding.check == check and fnmatch(finding.file, glob) and pattern.search(finding.message)
               for check, glob, pattern in rules)


COLORS = {"error": "\033[31m", "warn": "\033[33m", "info": "\033[36m"}
RESET = "\033[0m"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--spec-dir", default=None, help="default: <repo>/spec")
    ap.add_argument("--format", choices=("text", "json", "github"), default="text")
    ap.add_argument("--severity", choices=("error", "warn", "info"), default="info",
                    help="lowest severity to report (default: info)")
    ap.add_argument("--only", action="append", default=[], metavar="CHECK",
                    help="report only this check; repeatable")
    ap.add_argument("--strict", action="store_true", help="exit non-zero on warnings too")
    ap.add_argument("--no-ignore", action="store_true", help="ignore the ignore file")
    ap.add_argument("--list-checks", action="store_true")
    args = ap.parse_args()

    if args.list_checks:
        width = max(len(c) for c in CHECKS)
        for name, (sev, desc) in sorted(CHECKS.items(), key=lambda kv: (kv[1][0], kv[0])):
            print(f"{sev:<5}  {name:<{width}}  {desc}")
        return 0

    here = Path(__file__).resolve().parent
    repo_root = here.parent
    spec_dir = Path(args.spec_dir).resolve() if args.spec_dir else repo_root / "spec"
    if not spec_dir.is_dir():
        print(f"spec-lint: no spec directory at {spec_dir}", file=sys.stderr)
        return 2

    linter = Linter(spec_dir, repo_root)
    findings = linter.run()

    rules = [] if args.no_ignore else load_ignores(spec_dir / ".spec-lint-ignore")
    suppressed = sum(1 for f in findings if ignored(f, rules))
    findings = [f for f in findings if not ignored(f, rules)]

    floor = {"error": 0, "warn": 1, "info": 2}[args.severity]
    order = {"error": 0, "warn": 1, "info": 2}
    findings = [f for f in findings if order[f.severity] <= floor]
    if args.only:
        findings = [f for f in findings if f.check in args.only]

    counts = defaultdict(int)
    for f in findings:
        counts[f.severity] += 1

    if args.format == "json":
        print(json.dumps({
            "spec_dir": str(spec_dir),
            "documents": len(linter.docs),
            "ids": len(linter.definitions),
            "parameters": len(linter.params),
            "suppressed": suppressed,
            "counts": dict(counts),
            "findings": [{"check": f.check, "severity": f.severity, "file": f.file,
                          "line": f.line, "message": f.message} for f in findings],
        }, indent=2))
    elif args.format == "github":
        for f in findings:
            level = {"error": "error", "warn": "warning", "info": "notice"}[f.severity]
            print(f"::{level} file=spec/{f.file},line={f.line},title={f.check}::{f.message}")
    else:
        tty = sys.stdout.isatty()
        print(f"spec-lint: {len(linter.docs)} documents · {len(linter.definitions)} IDs · "
              f"{len(linter.params)} parameters · {len(linter.adr_files)} ADRs\n")
        width = max((len(f.check) for f in findings), default=0)
        current = None
        for f in findings:
            if f.severity != current:
                current = f.severity
                print()
            color = COLORS[f.severity] if tty else ""
            reset = RESET if tty else ""
            print(f"{color}{f.severity:<5}{reset} {f.check:<{width}}  "
                  f"spec/{f.file}:{f.line}  {f.message}")
        print()
        summary = (f"{counts['error']} errors · {counts['warn']} warnings · "
                   f"{counts['info']} info")
        if suppressed:
            summary += f" · {suppressed} suppressed"
        print(summary)

    if counts["error"]:
        return 1
    if args.strict and counts["warn"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
