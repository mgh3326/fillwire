"""Record the pure KR fill projection from auto_trader PR #2273, offline.

Usage: python3 record_fills.py /path/to/auto_trader > fills_recorded.json
Only git show is used; no checkout, database, credentials or broker is needed.
"""
import ast
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

COMMIT = "ac5e8676f04f4f5bcdb3f6d2c978a55bc60afbbd"
namespace = {}

def load(path, functions, classes=(), constants=()):
    source = subprocess.check_output(["git", "show", f"{COMMIT}:{path}"], cwd=sys.argv[1], text=True)
    tree = ast.parse(source)
    nodes = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)]
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(("app", "sqlalchemy")):
                continue
            nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in (*functions, *classes):
            nodes.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in constants for target in node.targets):
            nodes.append(node)
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(module, path, "exec"), namespace)

load("app/core/timezone.py", ["trade_day_kst"], constants=["KST"])
load("app/services/execution_ledger/supersede.py", ["normalize_order_id", "_instrument", "_symbol_key", "_aware", "order_trading_day", "order_identity", "is_authoritative", "covered_identities", "is_superseded", "supersede_provisional"], classes=["LedgerFillLike"], constants=["PROVISIONAL_SOURCE", "AUTHORITATIVE_SOURCES", "OrderIdentity"])
load("app/services/execution_intent_gateway/query.py", ["_order_number", "_whole_shares", "intent_fill_orders"])
now = datetime(2026, 11, 2, tzinfo=timezone.utc)
child = SimpleNamespace(child_id=1, action="submit", broker_order_id="000123", created_at=now)
row = SimpleNamespace(id=42, broker="kis", account_mode="mock", venue="krx", instrument_type="equity_kr", symbol="005930", side="buy", currency="KRW", broker_order_id="123", source="websocket", filled_qty=Decimal("2"), filled_price=Decimal("70000.00000000"), filled_at=now + timedelta(seconds=1), created_at=now + timedelta(seconds=2), quarantined_at=None)
orders = namespace["intent_fill_orders"]([child], [row])
json.dump({"intent_id":"3f2b6c1e-8a4d-4e5f-9a7b-1c2d3e4f5a6b", "orders":orders}, sys.stdout, indent=2)
print()
