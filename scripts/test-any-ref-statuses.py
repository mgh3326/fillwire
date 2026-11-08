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


def recorded_run(run_id: int = 12, attempt: int = 1, state: str = "completed") -> dict:
    return {
        "id": run_id,
        "run_attempt": attempt,
        "display_title": "any-ref sha",
        "status": state,
        "html_url": f"https://example.invalid/run/{run_id}",
    }


class VerdictTests(unittest.TestCase):
    def verdict(self, jobs: list[dict]) -> str:
        return statuses.verdicts(jobs, ["verify"])["verify"][0]

    def test_success_is_green(self) -> None:
        self.assertEqual(self.verdict([job("verify", "success")]), "success")

    def test_target_title_requires_canonical_lowercase_sha(self) -> None:
        self.assertEqual(statuses.target_from_title("any-ref " + "a" * 40), "a" * 40)
        self.assertIsNone(statuses.target_from_title("any-ref " + "A" * 40))

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

        run_details = {
            "id": 11,
            "run_attempt": 1,
            "display_title": "any-ref sha",
            "event": "workflow_dispatch",
            "head_branch": "main",
            "head_repository": {"full_name": "owner/repo"},
            "repository": {"full_name": "owner/repo"},
            "path": ".github/workflows/ci-any-ref.yml@refs/heads/main",
        }
        with patch.object(statuses, "_run_details", return_value=run_details), patch.object(
            statuses, "_is_ancestor", return_value=True
        ), patch.object(statuses, "_dispatch_runs", return_value=run_list), patch.object(
            statuses, "_fetch_jobs", side_effect=OSError("unavailable")
        ):
            with tempfile.TemporaryDirectory() as temp_dir, redirect_stderr(StringIO()):
                result = statuses.main(
                    ["--mode", "post", "--repo", "owner/repo", "--run-id", "11",
                     "--run-attempt", "1", "--run-url", "https://example.invalid/run",
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
        run_list = [recorded_run()]
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

            with patch.object(statuses, "_download_record", return_value=["verify", "secrets"]), patch.object(
                statuses, "_newer_published_contexts", return_value=set()
            ):
                result = statuses.reconcile_newer_poster(
                    "owner/repo", "sha", 11, 1, ["verify", "secrets"], run_list,
                    lambda *_: newer_jobs, publish, posted_file,
                )
            self.assertEqual(result, (True, (12, 1)))
            self.assertEqual(
                posted,
                [
                    ("verify", "success", "https://example.invalid/run/12/attempts/1"),
                    ("secrets", "failure", "https://example.invalid/run/12/attempts/1"),
                ],
            )
            self.assertEqual(posted_file.read_text(encoding="utf-8").splitlines(), ["verify", "secrets"])

    def test_failed_reconciliation_clears_old_post_markers(self) -> None:
        run_list = [recorded_run()]
        newer_jobs = [
            job("validate ref", "success"),
            job("record any-ref target", "success"),
            job("verify", "success"),
            job("secrets", "failure"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            posted_file = Path(temp_dir) / "posted.txt"
            posted_file.write_text("verify\nsecrets\n", encoding="utf-8")
            calls = 0

            def publish(*_):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return True
                raise OSError("temporary status API failure")

            with patch.object(statuses, "_download_record", return_value=["verify", "secrets"]), patch.object(
                statuses, "_newer_published_contexts", return_value=set()
            ):
                with self.assertRaises(RuntimeError):
                    statuses.reconcile_newer_poster(
                        "owner/repo", "sha", 11, 1, ["verify", "secrets"], run_list,
                        lambda *_: newer_jobs, publish, posted_file,
                    )
            self.assertEqual(posted_file.read_text(encoding="utf-8"), "verify\n")
            self.assertEqual(statuses.contexts_needing_failure(["verify", "secrets"], posted_file), ["secrets"])

    def test_fallback_preserves_contexts_already_posted_by_newer_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            posted_file = Path(temp_dir) / "posted.txt"
            posted_file.write_text("", encoding="utf-8")
            self.assertEqual(
                statuses.contexts_needing_failure(
                    ["verify", "secrets"], posted_file, {"verify", "secrets"}
                ),
                [],
            )
            self.assertEqual(
                statuses.contexts_needing_failure(["verify", "secrets"], posted_file, {"verify"}),
                ["secrets"],
            )

    def test_missing_record_produces_red_verdicts(self) -> None:
        jobs = [
            job("validate ref", "success"),
            job("record any-ref target", "failure"),
            job("verify", "success"),
            job("secrets", "success"),
        ]
        self.assertEqual(
            statuses._plan_for_run("owner/repo", 11, jobs, ["verify", "secrets"]),
            {
                "verify": ("failure", "validation or target recording did not succeed"),
                "secrets": ("failure", "validation or target recording did not succeed"),
            },
        )

    def test_record_failure_still_publishes_failure_for_known_target(self) -> None:
        run = recorded_run(run_id=11)
        run["display_title"] = "any-ref sha"
        jobs = [
            job("validate ref", "success"),
            job("record any-ref target", "failure"),
            job("verify", "success"),
            job("secrets", "success"),
        ]
        posted = []
        with tempfile.TemporaryDirectory() as temp_dir:

            def publish(repo, sha, context, state, reason, url):
                posted.append((context, state))
                return True

            with patch.object(statuses, "_dispatch_runs", return_value=[]), patch.object(
                statuses, "_fetch_jobs", return_value=jobs
            ):
                posted_file = Path(temp_dir) / "posted.txt"
                result = statuses._post_run(
                    "owner/repo", run, "sha", ["verify", "secrets"], posted_file, publish
                )
        self.assertEqual(result, 0)
        self.assertEqual(posted, [("verify", "failure"), ("secrets", "failure")])

    def test_fallback_does_not_replace_a_newer_run_status(self) -> None:
        run = recorded_run(run_id=11)
        run["display_title"] = "any-ref sha"
        newer = recorded_run()
        jobs = [job("validate ref", "success"), job("record any-ref target", "success")]
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            statuses, "_dispatch_runs", return_value=[newer]
        ), patch.object(statuses, "_fetch_jobs", return_value=jobs), patch.object(
            statuses, "_newer_published_contexts", return_value={"verify", "secrets"}
        ), patch.object(statuses, "_post") as post:
            result = statuses._fallback(
                "owner/repo", "sha", run, ["verify", "secrets"], Path(temp_dir) / "posted.txt"
            )
        self.assertEqual(result, 0)
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
