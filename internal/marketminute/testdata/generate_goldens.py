#!/usr/bin/env python3
"""Regenerate the Go golden fixtures from auto_trader's Python collector.

The rows and session labels below come from the real functions
``session_minutes_collector.bars_from_frame`` and
``session_segment.session_segment_of`` at auto_trader origin/main; this script
only serialises their output in the Go shadow row shape. Run it from a checkout
of auto_trader's origin/main with this directory's parent as the working
inscribing path:

    PYTHONPATH=/path/to/auto_trader python3 generate_goldens.py <out-dir>

The checked-in ``bars/*.input.json`` + ``bars/*.golden.jsonl`` and
``segments.golden.json`` are the output of this script.

Serialisation contract for one row (exactly these keys, in this order, compact
JSON, one line per row):

    time_utc, session_date, symbol, venue, session_segment, source,
    open, high, low, close, volume, value

Decimal fields are exact decimal strings (``str(Decimal)``), ``value`` is null
when the per-minute value cannot be proven. ``time_utc`` is RFC3339 UTC with a
``Z`` suffix.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from app.services.research_candles import session_minutes_collector as sc
from app.services.research_candles.session_segment import session_segment_of

KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")

ROW_KEYS = (
    "time_utc",
    "session_date",
    "symbol",
    "venue",
    "session_segment",
    "source",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
)


def rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def dec(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def row_dict(bar: sc.MinuteBar) -> dict[str, object]:
    return {
        "time_utc": rfc3339(bar.time_utc),
        "session_date": bar.session_date.isoformat(),
        "symbol": bar.symbol,
        "venue": bar.venue,
        "session_segment": bar.session_segment,
        "source": bar.source,
        "open": dec(bar.open),
        "high": dec(bar.high),
        "low": dec(bar.low),
        "close": dec(bar.close),
        "volume": dec(bar.volume),
        "value": dec(bar.value),
    }


def frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def json_records(data: pd.DataFrame) -> list[dict[str, object]]:
    """Frame records with pandas NaN turned into JSON null and numbers into strings."""
    records = []
    for record in data.to_dict("records"):
        clean = {}
        for key, value in record.items():
            if value is None or (isinstance(value, float) and math.isnan(value)):
                clean[key] = None
            elif isinstance(value, str):
                clean[key] = value
            else:
                clean[key] = str(value)
        records.append(clean)
    return records


def row(label: str, base: int = 100) -> dict[str, object]:
    return {
        "datetime": label,
        "open": str(base),
        "high": str(base + 1),
        "low": str(base - 1),
        "close": str(base),
        "volume": "10",
        "value": "1000",
    }


def rows(labels: list[str], base: int = 100, values: list[object] | None = None) -> list[dict[str, object]]:
    out = []
    for index, label in enumerate(labels):
        item = row(label, base + index)
        if values is not None:
            item["value"] = None if values[index] is None else str(values[index])
        out.append(item)
    return out


def kr_rows(labels: list[str], cumulative: list[int]) -> list[dict[str, object]]:
    return rows(labels, values=cumulative)


def us_rows(labels: list[str], cumulative: list[int]) -> list[dict[str, object]]:
    return rows(labels, values=cumulative)


BARS = (
    (
        "kr_forming_excluded",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 11, 24, 9, 2, 5, tzinfo=KST)),
        frame(rows(["2026-11-24 09:00", "2026-11-24 09:01", "2026-11-24 09:02"])),
    ),
    (
        "kr_closed_exactly_at_70s",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 11, 24, 9, 1, 10, tzinfo=KST)),
        frame(rows(["2026-11-24 09:00"])),
    ),
    (
        "kr_closed_one_second_early",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 11, 24, 9, 1, 9, tzinfo=KST)),
        frame(rows(["2026-11-24 09:00"])),
    ),
    (
        "kr_start_label_kst",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 11, 24, 12, 0, tzinfo=UTC)),
        frame(rows(["2026-11-24 09:00"])),
    ),
    (
        "kr_accumulated_difference",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(
            ["2026-11-24 09:00", "2026-11-24 09:01", "2026-11-24 09:02", "2026-11-24 09:05"],
            [100, 250, 250, 400],
        )),
    ),
    (
        "kr_ntx_first_row_null_value",
        dict(market="kr", symbol="005930", venue="NTX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(["2026-11-24 08:03", "2026-11-24 08:04"], [40, 55])),
    ),
    (
        "kr_oldest_row_of_long_frame_null",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(
            [f"2026-11-24 10:{minute:02d}" for minute in range(30)],
            [1000 + 10 * i for i in range(30)],
        )),
    ),
    (
        "kr_decreasing_accumulated",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(
            ["2026-11-24 09:00", "2026-11-24 09:01", "2026-11-24 09:02", "2026-11-24 09:03"],
            [500, 400, 450, 470],
        )),
    ),
    (
        "kr_row_after_dropped_invalid",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame([
            *kr_rows(
                ["2026-11-24 09:00", "2026-11-24 09:01", "2026-11-24 09:02", "2026-11-24 09:03"],
                [100, 250, 400, 460],
            )[:1],
            {**kr_rows(["2026-11-24 09:01"], [250])[0], "close": None},
            *kr_rows(["2026-11-24 09:02", "2026-11-24 09:03"], [400, 460]),
        ]),
    ),
    (
        "kr_invalid_first_row",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame([
            {**kr_rows(["2026-11-24 09:00"], [100])[0], "open": None},
            *kr_rows(["2026-11-24 09:01"], [250]),
        ]),
    ),
    (
        "kr_unreadable_value_breaks_chain",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(
            ["2026-11-24 09:00", "2026-11-24 09:01", "2026-11-24 09:02"],
            [100, None, 400],
        )),
    ),
    (
        "kr_accumulation_cross_day",
        dict(market="kr", symbol="005930", venue="KRX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(["2026-11-23 15:29", "2026-11-24 09:00"], [9000, 70])),
    ),
    (
        "kr_ntx_first_minute_0800",
        dict(market="kr", symbol="005930", venue="NTX",
             observed_before=datetime(2026, 12, 1, tzinfo=UTC)),
        frame(kr_rows(["2026-11-24 08:00", "2026-11-24 08:01"], [30, 80])),
    ),
    (
        "kr_invalid_and_duplicate_rows",
        dict(market="kr", symbol="123456", venue="KRX",
             observed_before=datetime(2026, 11, 24, 6, 0, tzinfo=UTC)),
        frame([
            *kr_rows(["2026-11-24 09:00"], [100]),
            *kr_rows(["2026-11-24 09:00"], [100]),
            {**kr_rows(["2026-11-24 09:01"], [250])[0], "close": None},
        ]),
    ),
    (
        "us_et_labels",
        dict(market="us", symbol="NVDA", venue="NASD",
             observed_before=datetime(2027, 1, 1, tzinfo=UTC)),
        frame(us_rows(
            ["2026-07-15 09:30", "2026-07-15 04:00", "2026-12-15 09:30", "2026-12-15 16:00"],
            [700, 800, 900, 1000],
        )),
    ),
    (
        "us_session_date_rollover",
        dict(market="us", symbol="NVDA", venue="NASD",
             observed_before=datetime(2027, 1, 1, tzinfo=UTC)),
        frame(us_rows(["2026-12-15 19:59", "2026-12-15 20:00"], [700, 800])),
    ),
    (
        "us_forming_excluded",
        dict(market="us", symbol="NVDA", venue="NASD",
             observed_before=datetime(2026, 11, 24, 11, 59, 30, tzinfo=UTC)),
        frame(us_rows(["2026-11-24 06:58", "2026-11-24 06:59"], [700, 800])),
    ),
)

US_EDT_DAY = (2026, 7, 15)
US_EST_DAY = (2026, 12, 15)
US_EARLY_CLOSE = (2026, 11, 27)
US_HOLIDAY = (2026, 11, 26)
KR_DAY = (2026, 11, 24)
KR_HOLIDAY = (2026, 12, 25)


def kst(day: tuple[int, int, int], hhmm: str) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    return datetime(*day, hour, minute, tzinfo=KST)


def et(day: tuple[int, int, int], hhmm: str) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    return datetime(*day, hour, minute, tzinfo=ET)


KR_CASES = [
    ("07:59", "UNKNOWN", "UNKNOWN"),
    ("08:00", "UNKNOWN", "NXT_PRE"),
    ("08:49", "UNKNOWN", "NXT_PRE"),
    ("08:50", "UNKNOWN", "UNKNOWN"),
    ("08:59", "UNKNOWN", "UNKNOWN"),
    ("09:00", "KRX_REGULAR", "NXT_OVERLAP"),
    ("15:19", "KRX_REGULAR", "NXT_OVERLAP"),
    ("15:20", "KRX_REGULAR", "UNKNOWN"),
    ("15:29", "KRX_REGULAR", "UNKNOWN"),
    ("15:30", "UNKNOWN", "UNKNOWN"),
    ("15:39", "UNKNOWN", "UNKNOWN"),
    ("15:40", "UNKNOWN", "UNKNOWN"),
    ("15:59", "UNKNOWN", "UNKNOWN"),
    ("16:00", "KRX_AFTER", "NXT_POST"),
    ("19:59", "KRX_AFTER", "NXT_POST"),
    ("20:00", "UNKNOWN", "UNKNOWN"),
]

US_CASES = [
    ("03:59", "UNKNOWN"),
    ("04:00", "US_PRE"),
    ("09:29", "US_PRE"),
    ("09:30", "US_REGULAR"),
    ("15:59", "US_REGULAR"),
    ("16:00", "US_POST"),
    ("19:59", "US_POST"),
    ("20:00", "UNKNOWN"),
]


def segments() -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for clock, krx, ntx in KR_CASES:
        out.append({"market": "kr", "venue": "KRX", "at": rfc3339(kst(KR_DAY, clock)),
                    "naive": False, "expected": krx})
        out.append({"market": "kr", "venue": "NTX", "at": rfc3339(kst(KR_DAY, clock)),
                    "naive": False, "expected": ntx})
    for clock, _, _ in KR_CASES:
        out.append({"market": "kr", "venue": "KRX", "at": rfc3339(kst(KR_HOLIDAY, clock)),
                    "naive": False, "expected": "UNKNOWN"})
        out.append({"market": "kr", "venue": "NTX", "at": rfc3339(kst(KR_HOLIDAY, clock)),
                    "naive": False, "expected": "UNKNOWN"})
    out.append({"market": "kr", "venue": "KRX",
                "at": rfc3339(kst((2026, 11, 21), "10:00")), "naive": False,
                "expected": session_segment_of("kr", "KRX", kst((2026, 11, 21), "10:00"))})
    out.append({"market": "kr", "venue": "XXX", "at": rfc3339(kst(KR_DAY, "10:00")),
                "naive": False, "expected": "UNKNOWN"})
    for day in (US_EDT_DAY, US_EST_DAY):
        for clock, expected in US_CASES:
            for venue in ("NASD", "NYSE", "AMEX"):
                out.append({"market": "us", "venue": venue,
                            "at": rfc3339(et(day, clock)), "naive": False,
                            "expected": expected})
    for clock, expected in (("12:59", "US_REGULAR"), ("13:00", "US_POST")):
        out.append({"market": "us", "venue": "NASD",
                    "at": rfc3339(et(US_EARLY_CLOSE, clock)), "naive": False,
                    "expected": expected})
    out.append({"market": "us", "venue": "NASD",
                "at": rfc3339(et(US_HOLIDAY, "10:00")), "naive": False,
                "expected": "UNKNOWN"})
    out.append({"market": "us", "venue": "KRX",
                "at": rfc3339(et(US_EDT_DAY, "10:00")), "naive": False,
                "expected": "UNKNOWN"})
    return out


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: generate_goldens.py <out-dir>")
    out = Path(sys.argv[1])
    bars_dir = out / "bars"
    bars_dir.mkdir(parents=True, exist_ok=True)
    for name, meta, frame_rows in BARS:
        observed = meta["observed_before"]
        result = sc.bars_from_frame(
            frame_rows,
            market=meta["market"],
            symbol=meta["symbol"],
            venue=meta["venue"],
            observed_before=observed.astimezone(UTC),
        )
        input_payload = {
            "market": meta["market"],
            "symbol": meta["symbol"],
            "venue": meta["venue"],
            "observed_before": rfc3339(observed),
            "frame": json_records(frame_rows),
            "expect": {
                "skipped_forming": result.skipped_forming,
                "skipped_invalid": result.skipped_invalid,
                "null_value": result.null_value,
            },
        }
        (bars_dir / f"{name}.input.json").write_text(
            json.dumps(input_payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        lines = [
            json.dumps(row_dict(bar), ensure_ascii=False, separators=(",", ":"))
            for bar in result.bars
        ]
        (bars_dir / f"{name}.golden.jsonl").write_text(
            "".join(line + "\n" for line in lines), encoding="utf-8"
        )
    payload = {
        "generated_by": "auto_trader session_segment.session_segment_of",
        "calendar": {
            "kr_closed": ["2026-12-25"],
            "us_closed": ["2026-11-26"],
            "us_early_close": {"2026-11-27": "13:00"},
        },
        "cases": segments(),
    }
    (out / "segments.golden.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
