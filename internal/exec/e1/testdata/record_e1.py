"""Record E1's answers to fillwire's child requests (ROB-1457).

Runs auto_trader's real IntentGateway (app/services/execution_intent_gateway)
with E1's own test fakes (tests/services/execution_intent_gateway/support.py:
MemoryGatewayStore, FakeBroker, FakeKillSwitch). No database, no network, no
broker, no environment, no credential is read. Each scenario gets a fresh
store, so E1 decides every child against exactly the children before it.

    git -C ~/work/auto_trader archive <commit> app tests pyproject.toml | tar -x -C SRC
    env -i PATH=/usr/bin:/bin ~/work/auto_trader/.venv/bin/python -I \
        record_e1.py SRC <commit> e1_scenarios.json > e1_recorded.json

The Go contract tests (contract_test.go) serve the recorded answers to the
executor and assert that it sends exactly the recorded requests.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sys
from datetime import datetime


def main() -> None:
    src, commit, scenarios_path = sys.argv[1:4]
    sys.path.insert(0, src)

    from app.schemas.execution_intent import ExecutionIntentV1, parse_child_request
    from app.services.execution_intent_gateway.decisions import IntentSnapshot
    from app.services.execution_intent_gateway.gateway import IntentGateway
    from app.services.kis_mock_runner.control import KillMode
    from tests.services.execution_intent_gateway.support import (
        ENABLED,
        FakeBroker,
        FakeKillSwitch,
        MemoryGatewayStore,
    )

    def snapshot(raw: dict, account: str | None) -> IntentSnapshot:
        v = ExecutionIntentV1.model_validate(raw)  # the E2 intent is a valid E1 intent
        return IntentSnapshot(
            intent_id=v.intent_id,
            strategy=v.strategy,
            account=account or v.account,
            venue=v.venue,
            symbol=v.symbol,
            side=v.side,
            max_quantity=v.max_quantity,
            max_notional_krw=v.max_notional_krw,
            limit_price_krw=v.limit_price_krw,
            max_slippage_bps=v.max_slippage_bps,
            position_at_authorization=v.inventory.position_at_authorization,
            min_position_after=v.inventory.min_position_after,
            max_position_after=v.inventory.max_position_after,
            expires_at=v.expires_at,
        )

    async def run(scenario: dict) -> dict:
        e1 = scenario["e1"]
        intent = snapshot(scenario["intent"], e1.get("store_account"))
        store = (
            MemoryGatewayStore(intent)
            if e1.get("store_intent", True)
            else MemoryGatewayStore()
        )
        broker = FakeBroker()
        kill = FakeKillSwitch(KillMode.ACTIVE)
        now = datetime.fromisoformat(
            e1.get("now", "2026-11-02T00:00:30Z").replace("Z", "+00:00")
        )
        forwarders = {a: broker for a in e1.get("forwarders", ["kis_mock"])}
        gateway = IntentGateway(
            store=store,
            forwarders=forwarders,
            kill_switch=kill,
            environment=ENABLED if e1.get("enabled", True) else {},
            clock=lambda: now,
        )
        for prior in e1.get("prior", []):
            out = await gateway.handle(parse_child_request(prior), confirm=True)
            assert out.status == "accepted", out
        steps = []
        for step in scenario["steps"]:
            kill.mode = KillMode(step.get("kill_mode", "ACTIVE"))
            broker.accepted = step.get("broker") != "fail"
            broker.raises = RuntimeError("broker") if step.get("broker") == "raise" else None
            store.finish_error = RuntimeError("db") if step.get("finish_error") else None
            request = parse_child_request(step["request"])
            out = await gateway.handle(request, confirm=step.get("confirm", True))
            steps.append({**step, "response": dataclasses.asdict(out)})
        return {**scenario, "steps": steps, "broker_calls": [list(c) for c in broker.calls]}

    with open(scenarios_path) as f:
        scenarios = json.load(f)["scenarios"]
    recorded = [asyncio.run(run(s)) for s in scenarios]
    json.dump(
        {
            "about": "E1 answers recorded by record_e1.py; do not edit by hand.",
            "auto_trader_commit": commit,
            "scenarios": recorded,
        },
        sys.stdout,
        indent=1,
        sort_keys=True,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
