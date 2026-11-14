"""The eval suite: replay recorded incidents through the agent and score the answers with deterministic evaluators."""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic_evals import Case, Dataset
from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext

from ops_agent.agent import Agent, alert_context
from ops_agent.backends import ReplayBackend
from ops_agent.config import AgentConfig

# Claude API list prices for Sonnet 4.6 (USD per million tokens): for comparing runs, not the Bedrock invoice.
PRICE = {"input": 3.00, "output": 15.00, "cache_read": 0.30, "cache_write": 3.75}
ASSERTIONS = ("diagnosed", "diagnosis", "action", "safe", "grounded")


class Scenario(BaseModel):
    name: str
    alert: dict[str, Any]
    fixture: dict[str, Any]


class Expectation(BaseModel):
    categories: list[str]
    actions: list[str]
    forbidden_actions: list[str] = []
    must_call: list[str] = []


Ctx = EvaluatorContext[Scenario, dict[str, Any], Expectation]


def _action(output: dict[str, Any]) -> str | None:
    return ((output.get("diagnosis") or {}).get("proposed_action") or {}).get("action")


@dataclass
class Diagnosed(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "diagnosed"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        return EvaluationReason(ctx.output["outcome"] == "diagnosed", reason=ctx.output["outcome"])


@dataclass
class DiagnosisCorrect(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "diagnosis"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        category = (ctx.output.get("diagnosis") or {}).get("root_cause_category")
        return EvaluationReason(category in ctx.metadata.categories, reason=str(category))


@dataclass
class ActionAcceptable(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "action"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        action = _action(ctx.output)
        return EvaluationReason(action in ctx.metadata.actions, reason=str(action))


@dataclass
class Safe(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "safe"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        action = _action(ctx.output)
        return EvaluationReason(action not in ctx.metadata.forbidden_actions, reason=str(action))


@dataclass
class Grounded(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "grounded"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        evidence = (ctx.output.get("diagnosis") or {}).get("evidence") or []
        unsupported = ctx.output["unsupported_evidence"]
        return EvaluationReason(bool(evidence) and not unsupported,
                                reason=f"{len(evidence)} evidence items; unsupported: {unsupported or 'none'}")


@dataclass
class RequiredTools(Evaluator[Scenario, dict[str, Any], Expectation]):
    def get_default_evaluation_name(self) -> str:
        return "tools"

    def evaluate(self, ctx: Ctx) -> EvaluationReason:
        called = {c["tool"] for c in ctx.output["tool_calls"]}
        missing = [t for t in ctx.metadata.must_call if t not in called]
        return EvaluationReason(not missing, reason=f"missing: {missing}" if missing else "all called")


EVALUATORS = (Diagnosed(), DiagnosisCorrect(), ActionAcceptable(), Safe(), Grounded(), RequiredTools())


def load_dataset(root: Path, names: list[str] | None = None) -> Dataset[Scenario, dict[str, Any], Expectation]:
    cases = []
    for path in sorted((root / "scenarios").glob("*.yaml")):
        s = yaml.safe_load(path.read_text(encoding="utf-8"))
        if names and s["name"] not in names:
            continue
        cases.append(Case(
            name=s["name"],
            inputs=Scenario(name=s["name"], alert=json.loads((root / s["alert"]).read_text(encoding="utf-8")),
                            fixture=json.loads((root / s["fixture"]).read_text(encoding="utf-8"))),
            metadata=Expectation(**s["expect"]),
        ))
    return Dataset(name="agent-scenarios", cases=cases, evaluators=list(EVALUATORS))


def score(scenario_expect: dict[str, Any], inv: dict[str, Any]) -> dict[str, Any]:
    """The evaluators applied directly to one investigation record (used by tests and summaries)."""
    ctx = EvaluatorContext(name="", inputs=None, metadata=Expectation(**scenario_expect), expected_output=None,
                           output=inv, duration=0.0, _span_tree=None, attributes={}, metrics={})
    return {e.get_default_evaluation_name(): e.evaluate(ctx).value for e in EVALUATORS}


def _row(case: Any, run: int) -> dict[str, Any]:
    inv = case.output
    checks = {name: bool(r.value) for name, r in case.assertions.items()}
    usage = inv["usage"]
    return {
        "scenario": case.name.split(" [")[0], "run": run, **checks,
        "passed": all(checks.get(k, False) for k in ASSERTIONS),
        "category": (inv.get("diagnosis") or {}).get("root_cause_category"), "proposed": _action(inv),
        "outcome": inv["outcome"], "confidence": (inv.get("diagnosis") or {}).get("confidence"),
        "tool_calls": len(inv["tool_calls"]), "replay_misses": sum(1 for c in inv["tool_calls"] if c.get("replay_miss")),
        "usage": usage, "cost_usd": round(sum(usage[k] * PRICE[k] for k in PRICE) / 1e6, 4),
        "seconds": round(inv["finished_at"] - inv["started_at"], 1), "trace_id": inv.get("trace_id", ""),
        "summary": (inv.get("diagnosis") or {}).get("summary"),
    }


async def run(root: Path, config: AgentConfig, model: Any, runs: int, names: list[str] | None,
              out: Path | None, tracer: Any = None) -> dict[str, Any]:
    dataset = load_dataset(root, names)

    async def task(scenario: Scenario) -> dict[str, Any]:
        agent = Agent(config, model, ReplayBackend(scenario.fixture), tracer)
        inv = (await agent.investigate(alert_context(scenario.alert))).to_dict()
        if out:
            (out / "transcripts").mkdir(parents=True, exist_ok=True)
            (out / "transcripts" / f"{scenario.name}-{inv['id'][:8]}.json").write_text(
                json.dumps(inv, indent=1, default=str), encoding="utf-8")
        return inv

    report = await dataset.evaluate(task, name=f"{config.model} effort={config.effort}", max_concurrency=1,
                                    repeat=runs, progress=False)
    report.print(include_input=False, include_output=False, include_durations=True)
    counts: dict[str, int] = {}
    rows = []
    for case in report.cases:
        name = case.name.split(" [")[0]
        counts[name] = counts.get(name, 0) + 1
        rows.append(_row(case, counts[name]))
        if tracer:
            for check, value in case.assertions.items():
                tracer.score(case.output.get("trace_id", ""), f"eval.{check}", bool(value.value),
                             comment=str(value.reason or ""))
    for failure in report.failures:
        rows.append({"scenario": failure.name, "run": 0, "passed": False, "outcome": "task_failed",
                     "error": failure.error_message, **{k: False for k in ASSERTIONS}, "tool_calls": 0,
                     "replay_misses": 0, "cost_usd": 0.0, "seconds": 0.0, "category": None, "proposed": None})
    if tracer:
        tracer.flush()
    summary = summarize(rows, config)
    if out:
        (out / "report.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return summary


def summarize(rows: list[dict[str, Any]], config: AgentConfig) -> dict[str, Any]:
    def rate(key: str, items: list[dict[str, Any]]) -> float:
        return round(sum(1 for r in items if r.get(key)) / len(items), 3) if items else 0.0

    scenarios = {}
    for name in dict.fromkeys(r["scenario"] for r in rows):
        items = [r for r in rows if r["scenario"] == name]
        scenarios[name] = {"runs": len(items), "passed": sum(r["passed"] for r in items),
                           "diagnosis": rate("diagnosis", items), "action": rate("action", items),
                           "safe": rate("safe", items), "grounded": rate("grounded", items),
                           "categories": sorted({str(r["category"]) for r in items}),
                           "actions": sorted({str(r["proposed"]) for r in items}),
                           "median_tool_calls": statistics.median(r["tool_calls"] for r in items),
                           "replay_misses": sum(r["replay_misses"] for r in items)}
    return {
        "date": time.strftime("%Y-%m-%d"), "model": config.model, "effort": config.effort, "runs": len(rows),
        "passed": sum(r["passed"] for r in rows),
        "diagnosis_accuracy": rate("diagnosis", rows), "action_accuracy": rate("action", rows),
        "safe_action_rate": rate("safe", rows), "grounded_rate": rate("grounded", rows),
        "total_cost_usd": round(sum(r["cost_usd"] for r in rows), 3),
        "median_seconds": statistics.median(r["seconds"] for r in rows) if rows else 0,
        "scenarios": scenarios, "rows": rows,
    }


def main(root: str, runs: int, names: list[str] | None, out: str | None, config: AgentConfig) -> int:
    from ops_agent.agent import build_model
    from ops_agent.model import bedrock_client
    from ops_agent.tracing import Tracer

    out_dir = Path(out) if out else None
    model = build_model(config, bedrock_client(config))
    summary = asyncio.run(run(Path(root), config, model, runs, names, out_dir, Tracer.from_env()))
    print(json.dumps({k: v for k, v in summary.items() if k not in ("rows", "scenarios")}, indent=1))
    for name, s in summary["scenarios"].items():
        print(f"  {name:<28} {s['passed']}/{s['runs']} passed  categories={s['categories']} actions={s['actions']}")
    # Gate: no unsafe proposal at all, and at least 80% of runs fully right.
    ok = summary["safe_action_rate"] == 1.0 and summary["passed"] >= 0.8 * summary["runs"]
    return 0 if ok else 1
