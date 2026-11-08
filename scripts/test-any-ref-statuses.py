#!/usr/bin/env python3
"""Behavior tests for the any-ref status verdict and supersession rules."""

from __future__ import annotations

import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / ".github" / "scripts"))
import any_ref_statuses as statuses  # noqa: E402


def job(name: str, conclusion: str, state: str = "completed") -> dict:
    return {"name": name, "status": state, "conclusion": conclusion}


class VerdictTests(unittest.TestCase):
    def verdict(self, jobs: list[dict]) -> str:
        return statuses.verdicts(jobs, ["verify"])["verify"][0]

    def test_success_is_green(self) -> None:
        self.assertEqual(self.verdict([job("verify", "success")]), "success")

    def test_cancelled_is_red(self) -> None:
        self.assertEqual(self.verdict([job("verify", "cancelled")]), "failure")

    def test_skipped_is_red(self) -> None:
        self.assertEqual(self.verdict([job("verify", "skipped")]), "failure")

    def test_failure_is_red(self) -> None:
        self.assertEqual(self.verdict([job("verify", "failure")]), "failure")

    def test_missing_is_red(self) -> None:
        self.assertEqual(self.verdict([]), "failure")

    def test_duplicate_is_red(self) -> None:
        self.assertEqual(self.verdict([job("verify", "success"), job("verify", "success")]), "failure")

    def test_supersedes_only_after_validation_and_record_succeed(self) -> None:
        run_list = [{"id": 12, "run_attempt": 1, "display_title": "any-ref sha"}]
        recorded = [job("validate ref", "success"), job("record any-ref target", "success")]
        failed_record = [job("validate ref", "success"), job("record any-ref target", "failure")]
        failed_validation = [job("validate ref", "failure"), job("record any-ref target", "success")]
        self.assertEqual(
            statuses.supersession_decision(run_list, "any-ref sha", 11, 1, lambda *_: recorded),
            (True, (12, 1)),
        )
        self.assertEqual(
            statuses.supersession_decision(run_list, "any-ref sha", 11, 1, lambda *_: failed_record),
            (False, None),
        )
        self.assertEqual(
            statuses.supersession_decision(run_list, "any-ref sha", 11, 1, lambda *_: failed_validation),
            (False, None),
        )

    def test_unreadable_newer_run_fails_closed(self) -> None:
        run_list = [{"id": 12, "run_attempt": 1, "display_title": "any-ref sha"}]

        def unreadable(*_):
            raise OSError("unavailable")

        with self.assertRaises(statuses.UnreadableNewerRun):
            statuses.supersession_decision(run_list, "any-ref sha", 11, 1, unreadable)

        with patch.object(statuses, "_is_ancestor", return_value=True), patch.object(
            statuses, "_dispatch_runs", return_value=run_list
        ), patch.object(statuses, "_fetch_jobs", side_effect=OSError("unavailable")):
            with tempfile.TemporaryDirectory() as temp_dir, redirect_stderr(StringIO()):
                result = statuses.main(
                    ["--repo", "owner/repo", "--sha", "sha", "--run-id", "11",
                     "--run-attempt", "1", "--target-url", "https://example.invalid/run",
                     "--posted-file", str(Path(temp_dir) / "posted.txt"), "--watch", "verify"]
                )
        self.assertEqual(result, 1)

    def test_github_calls_retry_after_transient_errors(self) -> None:
        calls = 0

        def flaky_call() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise OSError("temporary API failure")
            return "ok"

        with patch.object(statuses.time, "sleep") as sleep:
            self.assertEqual(statuses._retry(flaky_call), "ok")
        self.assertEqual(calls, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_stale_poster_reconciles_newer_completed_verdict(self) -> None:
        run_list = [{
            "id": 12,
            "run_attempt": 1,
            "display_title": "any-ref sha",
            "status": "completed",
            "html_url": "https://example.invalid/run/12",
        }]
        newer_jobs = [
            job("validate ref", "success"),
            job("record any-ref target", "success"),
            job("verify", "success"),
            job("secrets", "failure"),
        ]
        posted = []
        with tempfile.TemporaryDirectory() as temp_dir:
            posted_file = Path(temp_dir) / "posted.txt"

            def publish(repo, sha, context, state, reason, url):
                posted.append((context, state, url))
                return True

            result = statuses.reconcile_newer_poster(
                "owner/repo", "sha", 11, 1, ["verify", "secrets"], run_list,
                lambda *_: newer_jobs, publish, posted_file,
            )
            self.assertEqual(result, (True, (12, 1)))
            self.assertEqual(
                posted,
                [
                    ("verify", "success", "https://example.invalid/run/12"),
                    ("secrets", "failure", "https://example.invalid/run/12"),
                ],
            )
            self.assertEqual(posted_file.read_text(encoding="utf-8").splitlines(), ["verify", "secrets"])


if __name__ == "__main__":
    unittest.main()
