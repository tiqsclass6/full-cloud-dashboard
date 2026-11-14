"""Command line: run either service, approve from a terminal, investigate, capture fixtures, run evals.

  python -m ops_agent agent                      serve ops-agent (Alertmanager webhook)
  python -m ops_agent control                    serve ops-control (approvals, executor, audit store)
  python -m ops_agent runtime                    the agent on Amazon Bedrock AgentCore Runtime (/ping, /invocations)
  python -m ops_agent proposals [--status S]     list proposals (inside ops-control)
  python -m ops_agent approve ID --hash H --approver NAME [--reject]   decide from a terminal
  python -m ops_agent investigate --alert FILE [--replay FIXTURE]       one investigation, printed
  python -m ops_agent capture --alert FILE --out FIXTURE [--investigation FILE]
  python -m ops_agent eval [--runs N] [--scenario NAME ...] [--out DIR]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path


def _control_service():
    from ops_agent.approvals import ApprovalService
    from ops_agent.config import ControlConfig
    from ops_agent.executor import KubeRunner
    from ops_agent.store import PostgresStore

    config = ControlConfig.from_env()
    store = PostgresStore(config.database_url)
    return config, store, lambda notifier=None: ApprovalService(config, store, KubeRunner(), notifier)


def serve_agent() -> None:
    import uvicorn

    from ops_agent.agent_app import build_app
    from ops_agent.config import AgentConfig

    uvicorn.run(build_app(AgentConfig.from_env()), host="0.0.0.0", port=int(os.environ.get("PORT", "8080")),
                log_level="warning", access_log=False)


def serve_runtime() -> None:
    import uvicorn

    from ops_agent.config import AgentConfig
    from ops_agent.runtime_app import build_app

    uvicorn.run(build_app(AgentConfig.from_env()), host="0.0.0.0", port=8080, log_level="warning", access_log=False)


def serve_control() -> None:
    import uvicorn

    from ops_agent.control_app import build_app

    config, store, make = _control_service()
    store.init()
    notifier = None
    if config.slack_bot_token and config.slack_app_token:
        from ops_agent.slack import SlackNotifier

        notifier = SlackNotifier(config.slack_bot_token, config.slack_app_token, config.slack_channel,
                                 config.grafana_url)
    service = make(notifier)
    if notifier:
        notifier.start(service)
    uvicorn.run(build_app(config, service), host="0.0.0.0", port=int(os.environ.get("PORT", "8080")),
                log_level="warning", access_log=False)


def approve(args: argparse.Namespace) -> int:
    _, store, make = _control_service()
    p = store.get_proposal(args.id)
    if p is None:
        print("no such proposal")
        return 1
    # A 12+ character prefix stands for the full hash; a mismatch still goes through decide() so it is audited.
    seen = p.params_hash if len(args.hash) >= 12 and p.params_hash.startswith(args.hash) else args.hash
    result = make().decide(p.id, args.approver, not args.reject, seen, via="cli")
    if not result.ok and seen != p.params_hash:
        print(f"hash does not match: the proposal is {p.action} {p.parameters} (hash {p.params_hash[:12]})")
    print(json.dumps(result.__dict__))
    return 0 if result.ok else 1


def proposals(args: argparse.Namespace) -> int:
    _, store, _ = _control_service()
    for p in store.list_proposals(args.status):
        print(f"{p.id}  {p.status:<9} {p.action:<24} {json.dumps(p.parameters)}  hash={p.params_hash[:12]}  "
              f"approvals={p.approvals}")
    return 0


async def _investigate(args: argparse.Namespace) -> int:
    from ops_agent.agent import Agent, alert_context, build_model
    from ops_agent.backends import McpBackend, ReplayBackend
    from ops_agent.config import AgentConfig
    from ops_agent.model import bedrock_client
    from ops_agent.tracing import Tracer

    config = AgentConfig.from_env()
    trigger = alert_context(json.loads(Path(args.alert).read_text(encoding="utf-8")))
    model, tracer = build_model(config, bedrock_client(config)), Tracer.from_env()
    if args.replay:
        inv = await Agent(config, model, ReplayBackend.load(args.replay), tracer).investigate(trigger)
    else:
        async with McpBackend(config.mcp_url, config.mcp_token) as backend:
            inv = await Agent(config, model, backend, tracer).investigate(trigger)
    if tracer:
        tracer.flush()
    record = inv.to_dict()
    if args.out:
        Path(args.out).write_text(json.dumps(record, indent=1, default=str), encoding="utf-8")
    print(json.dumps({k: record[k] for k in ("id", "outcome", "error", "turns", "usage", "unsupported_evidence")}))
    print(json.dumps(record["diagnosis"], indent=1))
    return 0 if inv.outcome == "diagnosed" else 1


async def _capture(args: argparse.Namespace) -> int:
    from ops_agent.backends import McpBackend
    from ops_agent.capture import capture
    from ops_agent.config import AgentConfig

    config = AgentConfig.from_env()
    # "-" reads the alert from stdin / writes the fixture to stdout (for kubectl exec).
    alert = json.loads(sys.stdin.read() if args.alert == "-" else Path(args.alert).read_text(encoding="utf-8"))
    extra = json.loads(Path(args.investigation).read_text(encoding="utf-8")) if args.investigation else None
    async with McpBackend(config.mcp_url, config.mcp_token) as backend:
        fixture = await capture(backend, alert, extra, args.source)
    text = json.dumps(fixture, indent=1)
    if args.out == "-":
        sys.stdout.write(text)
    else:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"captured {len(fixture['responses'])} tool results into {args.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr if "-" in (argv or sys.argv) else sys.stdout, level=logging.INFO,
                        format="%(message)s")
    for noisy in ("httpx", "httpx2", "mcp"):      # one INFO line per HTTP request otherwise
        logging.getLogger(noisy).setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(prog="ops_agent")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("agent")
    sub.add_parser("control")
    sub.add_parser("runtime")
    p = sub.add_parser("proposals")
    p.add_argument("--status")
    p = sub.add_parser("approve")
    p.add_argument("id")
    p.add_argument("--hash", required=True, help="the parameter hash shown with the proposal (12+ characters)")
    p.add_argument("--approver", required=True)
    p.add_argument("--reject", action="store_true")
    p = sub.add_parser("investigate")
    p.add_argument("--alert", required=True)
    p.add_argument("--replay")
    p.add_argument("--out")
    p = sub.add_parser("capture")
    p.add_argument("--alert", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--investigation")
    p.add_argument("--source", default="")
    p = sub.add_parser("eval")
    p.add_argument("--root", default="evals/agent-scenarios")
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--scenario", action="append")
    p.add_argument("--out")
    # No arguments: the mode comes from OPS_AGENT_MODE (AgentCore Runtime cannot pass a command).
    argv = argv if argv is not None else (sys.argv[1:] or [os.environ.get("OPS_AGENT_MODE", "agent")])
    args = parser.parse_args(argv)

    if args.command == "agent":
        serve_agent()
    elif args.command == "control":
        serve_control()
    elif args.command == "runtime":
        serve_runtime()
    elif args.command == "approve":
        return approve(args)
    elif args.command == "proposals":
        return proposals(args)
    elif args.command == "investigate":
        return asyncio.run(_investigate(args))
    elif args.command == "capture":
        return asyncio.run(_capture(args))
    elif args.command == "eval":
        from ops_agent.config import AgentConfig
        from ops_agent.evals import main as eval_main

        return eval_main(args.root, args.runs, args.scenario, args.out, AgentConfig.from_env())
    return 0


if __name__ == "__main__":
    sys.exit(main())
