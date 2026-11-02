# Committed-fill fixture

`fills_recorded.json` was generated offline by `record_fills.py` from the actual
pure `intent_fill_orders` projection and its KR supersession helpers in
auto_trader PR #2273 at `ac5e8676f04f4f5bcdb3f6d2c978a55bc60afbbd`.
The script reads that commit with `git show`, extracts the standard-library-only
functions, supplies synthetic KR journal/ledger objects, and serializes the route's
`intent_id`/`orders` envelope. It opens no database, broker or network connection.

Recreate it with Python 3.12+:

```sh
python3 record_fills.py /path/to/auto_trader > fills_recorded.json
```

`price` is an exact decimal string, the broker ID is on the enclosing order,
and `committed_at` is the ledger's stored `created_at` timestamp, not an exact
COMMIT clock. Consumers must refresh the whole snapshot. The executor stops for
reconcile when an upsert/supersession changes previously counted evidence; it
never adds replacements over old fills. Fractional KRW prices are refused by E2's
integer-KRW domain.
