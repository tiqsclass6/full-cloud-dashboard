"""Keyword search over the runbooks (docs/runbooks/*.md, copied into the image); one entry per `## ` section."""
from __future__ import annotations

import re
from pathlib import Path

_WORD = re.compile(r"[a-z0-9]+")


def _plain(heading: str) -> str:
    """Heading text without emphasis markers: `## **KafkaBrokerDown**` is the KafkaBrokerDown section,
    and its anchor (#kafkabrokerdown) is built from the text, as GitHub builds it."""
    return heading.replace("**", "").replace("__", "").strip()


def _slug(heading: str) -> str:
    """GitHub's anchor for a heading: lower case, punctuation dropped, spaces become hyphens."""
    return re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")


def _words(text: str) -> list[str]:
    # Split CamelCase alert names too: "KafkaBrokerDown" also matches "broker down".
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    return _WORD.findall(spaced.lower())


class RunbookIndex:
    def __init__(self, directory: str | Path) -> None:
        self.sections: list[dict[str, str]] = []
        for path in sorted(Path(directory).glob("*.md")):
            title, current, body = "", "", []
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("# "):
                    title = _plain(line[2:])
                elif line.startswith("## "):
                    self._add(path.name, title, current, body)
                    current, body = _plain(line[3:]), []
                elif current:
                    body.append(line)
            self._add(path.name, title, current, body)

    def _add(self, file: str, title: str, heading: str, body: list[str]) -> None:
        if heading:
            self.sections.append({"runbook": f"docs/runbooks/{file}#{_slug(heading)}", "title": title,
                                  "heading": heading, "text": "\n".join(body).strip()})

    def search(self, query: str, limit: int = 3) -> list[dict[str, str]]:
        exact = [s for s in self.sections if s["heading"].lower() == query.strip().lower()]
        if exact:
            return exact[:1]
        terms = set(_words(query))
        scored = []
        for s in self.sections:
            heading, text = set(_words(s["heading"])), _words(s["text"])
            score = 3 * len(terms & heading) + sum(1 for w in text if w in terms)
            if score:
                scored.append((score, s))
        scored.sort(key=lambda x: -x[0])
        return [{**s, "text": s["text"][:2500]} for _, s in scored[:limit]]
