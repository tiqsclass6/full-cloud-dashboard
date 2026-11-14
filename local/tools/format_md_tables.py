#!/usr/bin/env python3
"""Align GitHub-flavoured Markdown tables: even column widths and one space of padding inside borders.

  format_md_tables.py [--check] [paths...]     default: every .md file under the repository root
"""
import argparse
import os
import re
import sys
import unicodedata

SKIP_DIRS = {".git", ".venv", "node_modules", "build", ".gradle", ".terraform", "__pycache__", ".idea", ".vscode"}
ALIGN_CELL = re.compile(r"^:?-{1,}:?$")


def display_width(text: str) -> int:
    width = 0
    for ch in text:
        if unicodedata.combining(ch) or ch in "​‍️":
            continue
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return width


def split_row(line: str) -> list:
    """Splits a table row on unescaped pipes and strips the outer ones."""
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    cells, current, i = [], [], 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            current.append(text[i:i + 2])
            i += 2
            continue
        if ch == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    cells.append("".join(current).strip())
    return cells


def is_table_row(line: str) -> bool:
    return line.lstrip().startswith("|")


def format_table(lines: list) -> list:
    rows = [split_row(line) for line in lines]
    header, delimiter = rows[0], rows[1]
    if len(header) != len(delimiter) or not all(ALIGN_CELL.match(c) for c in delimiter):
        return lines  # not a real table (no valid delimiter row): leave alone
    columns = len(header)
    if any(len(r) != columns for r in rows):
        return lines  # ragged rows: do not guess
    aligns = []
    for cell in delimiter:
        left, right = cell.startswith(":"), cell.endswith(":")
        aligns.append("center" if left and right else "right" if right else "left" if left else "none")
    body = [rows[0]] + rows[2:]
    widths = [max(3, *(display_width(r[c]) for r in body)) for c in range(columns)]

    def pad(text, width, align):
        gap = width - display_width(text)
        if align == "right":
            return " " * gap + text
        if align == "center":
            return " " * (gap // 2) + text + " " * (gap - gap // 2)
        return text + " " * gap

    def delim(width, align):
        if align == "center":
            return ":" + "-" * (width - 2) + ":"
        if align == "right":
            return "-" * (width - 1) + ":"
        if align == "left":
            return ":" + "-" * (width - 1)
        return "-" * width

    out = []
    for index, row in enumerate(rows):
        if index == 1:
            cells = [delim(widths[c], aligns[c]) for c in range(columns)]
        else:
            cells = [pad(row[c], widths[c], aligns[c]) for c in range(columns)]
        out.append("| " + " | ".join(cells) + " |")
    return out


def format_text(text: str) -> str:
    lines = text.split("\n")
    result, i, in_fence = [], 0, False
    while i < len(lines):
        line = lines[i]
        if re.match(r"^\s*(```|~~~)", line):
            in_fence = not in_fence
            result.append(line)
            i += 1
            continue
        if not in_fence and is_table_row(line):
            j = i
            while j < len(lines) and is_table_row(lines[j]):
                j += 1
            block = lines[i:j]
            if len(block) >= 2:
                formatted = format_table(block)
                before = [split_row(b) for k, b in enumerate(block) if k != 1]
                after = [split_row(b) for k, b in enumerate(formatted) if k != 1]
                result.extend(formatted if before == after else block)
            else:
                result.extend(block)
            i = j
            continue
        result.append(line)
        i += 1
    return "\n".join(result)


def markdown_files(roots):
    for root in roots:
        if os.path.isfile(root):
            yield root
            continue
        for directory, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for name in files:
                if name.lower().endswith(".md"):
                    yield os.path.join(directory, name)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    here = os.path.dirname(os.path.abspath(__file__))
    roots = args.paths or [os.path.normpath(os.path.join(here, "..", ".."))]
    changed = []
    for path in sorted(markdown_files(roots)):
        with open(path, encoding="utf-8", newline="") as handle:
            original = handle.read()
        newline = "\r\n" if "\r\n" in original else "\n"
        updated = format_text(original.replace("\r\n", "\n")).replace("\n", newline)
        if updated != original:
            changed.append(path)
            if not args.check:
                with open(path, "w", encoding="utf-8", newline="") as handle:
                    handle.write(updated)
    for path in changed:
        print(("would reformat " if args.check else "reformatted ") + os.path.relpath(path))
    print(f"{len(changed)} file(s) {'need' if args.check else 'were'} reformatted")
    sys.exit(1 if (args.check and changed) else 0)


if __name__ == "__main__":
    main()
