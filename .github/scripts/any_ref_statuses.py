#!/usr/bin/env python3
"""Publish and recover watched statuses for a main-dispatched any-ref run."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

TARGET_BRANCH = "main"
WORKFLOW_FILE = ".github/workflows/ci-any-ref.yml"
VALIDATE_JOB = "validate ref"
RECORD_JOB = "record any-ref target"
ARTIFACT_NAME = "any-ref-target"
DESCRIPTION_LIMIT = 140
JOBS_PAGE_SIZE = 100
API_RETRY_ATTEMPTS = 4

JobFetch = Callable[[int, int], list[dict[str, Any]]]
StatusPost = Callable[[str, str, str, str, str, str], bool]


class UnreadableNewerRun(RuntimeError):
    """A newer run may publish, but its supersession state could not be read."""


def job_matches(job_name: str, watched: str) -> bool:
    """Match a direct job or a job inside a reusable workflow."""
    return job_name == watched or job_name.endswith(" / " + watched)


def run_title(sha: str) -> str:
    return f"any-ref {sha}"


def target_from_title(title: str) -> str | None:
    match = re.fullmatch(r"any-ref ([0-9a-f]{40})", title)
    return match.group(1) if match else None


def record_matches_target(recorded_sha: str, target_sha: str) -> bool:
    return (
        target_from_title(run_title(recorded_sha)) == recorded_sha
        and recorded_sha == target_sha
    )


def attempt_url(run: dict[str, Any]) -> str:
    return f"{str(run.get('html_url', '')).rstrip('/')}/attempts/{run.get('run_attempt', 1)}"


def newer_runs(
    runs: list[dict[str, Any]], title: str, run_id: int, run_attempt: int
) -> list[tuple[int, int]]:
    """Return matching run attempts newer than this one, newest first."""
    found = []
    for run in runs:
        if run.get("display_title") != title:
            continue
        candidate_id = int(run["id"])
        candidate_attempt = int(run.get("run_attempt", 1))
        if candidate_id > run_id or (
            candidate_id == run_id and candidate_attempt > run_attempt
        ):
            found.append((candidate_id, candidate_attempt))
    return sorted(found, reverse=True)


def records_target(jobs: list[dict[str, Any]]) -> bool:
    """A run supersedes another poster only after validation and record upload."""

    def succeeded(name: str) -> bool:
        found = [job for job in jobs if job.get("name") == name]
        return len(found) == 1 and found[0].get("conclusion") == "success"

    return succeeded(VALIDATE_JOB) and succeeded(RECORD_JOB)


def supersession_decision(
    runs: list[dict[str, Any]],
    title: str,
    run_id: int,
    run_attempt: int,
    fetch_jobs: JobFetch,
) -> tuple[bool, tuple[int, int] | None]:
    """A newer recorded target supersedes; unreadable newer runs fail closed."""
    unreadable = []
    for newer_id, newer_attempt in newer_runs(runs, title, run_id, run_attempt):
        try:
            if records_target(fetch_jobs(newer_id, newer_attempt)):
                return True, (newer_id, newer_attempt)
        except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError) as error:
            unreadable.append(f"run {newer_id} attempt {newer_attempt} ({type(error).__name__})")
    if unreadable:
        raise UnreadableNewerRun(
            "could not read the state of a newer run, so this run cannot tell whether it is "
            f"superseded; retry is required: {', '.join(unreadable)}"
        )
    return False, None


def verdicts(jobs: list[dict[str, Any]], watched: list[str]) -> dict[str, tuple[str, str]]:
    """Only one completed successful job is green; all ambiguity fails closed."""
    result = {}
    for name in watched:
        found = [job for job in jobs if job_matches(str(job.get("name", "")), name)]
        if len(found) != 1:
            result[name] = ("failure", f"{len(found)} jobs match, expected one")
            continue
        job = found[0]
        green = job.get("status") == "completed" and job.get("conclusion") == "success"
        reason = str(job.get("conclusion") or job.get("status") or "unknown")
        result[name] = ("success" if green else "failure", reason)
    return result


def failed_verdicts(watched: list[str], reason: str) -> dict[str, tuple[str, str]]:
    return {name: ("failure", reason) for name in watched}


def description(name: str, reason: str) -> str:
    return f"any-ref CI {name}: {reason}"[:DESCRIPTION_LIMIT]


def _retry(operation):
    """Retry transient GitHub CLI/API failures with short bounded backoff."""
    for attempt in range(API_RETRY_ATTEMPTS):
        try:
            return operation()
        except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError):
            if attempt + 1 == API_RETRY_ATTEMPTS:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def _gh_json(args: list[str]) -> Any:
    def request() -> Any:
        out = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True).stdout
        return json.loads(out)

    return _retry(request)


def _run_details(repo: str, run_id: str, attempt: int) -> dict[str, Any]:
    return _gh_json([f"repos/{repo}/actions/runs/{run_id}/attempts/{attempt}"])


def _fetch_jobs(repo: str, run_id: int, attempt: int) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    page = 1
    while True:
        batch = _gh_json(
            [f"repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page={JOBS_PAGE_SIZE}&page={page}"]
        ).get("jobs", [])
        jobs.extend(batch)
        if len(batch) < JOBS_PAGE_SIZE:
            return jobs
        page += 1


def _dispatch_runs(repo: str) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    page = 1
    while True:
        batch = _gh_json(
            [f"repos/{repo}/actions/workflows/ci-any-ref.yml/runs?event=workflow_dispatch&branch={TARGET_BRANCH}&per_page={JOBS_PAGE_SIZE}&page={page}"]
        ).get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < JOBS_PAGE_SIZE:
            return runs
        page += 1


def _is_ancestor(repo: str, sha: str) -> bool:
    status = _gh_json([f"repos/{repo}/compare/{TARGET_BRANCH}...{sha}"]).get("status")
    return status in {"behind", "identical"}


def _latest_context_statuses(
    statuses: list[dict[str, Any]], watched: list[str]
) -> dict[str, dict[str, Any]]:
    """The status API is newest-first; keep the first status for each context."""
    latest: dict[str, dict[str, Any]] = {}
    for status in statuses:
        context = str(status.get("context", ""))
        if context in watched and context not in latest:
            latest[context] = status
    return latest


def _download_record(
    repo: str, run_id: int, attempt: int, target_sha: str, watched: list[str]
) -> list[str]:
    def request() -> list[str]:
        with tempfile.TemporaryDirectory(prefix="any-ref-target-") as directory:
            subprocess.run(
                [
                    "gh", "run", "download", str(run_id), "--repo", repo,
                    "--name", f"{ARTIFACT_NAME}-{attempt}", "--dir", directory,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            target_dir = Path(directory)
            sha = (target_dir / "sha.txt").read_text(encoding="utf-8").strip()
            if not record_matches_target(sha, target_sha):
                raise ValueError("recorded target must match the run title's lowercase 40-hex SHA")
            names = (target_dir / "expected-checks.txt").read_text(encoding="utf-8").splitlines()
            if sorted(names) != sorted(watched):
                raise ValueError("recorded check names differ from the names this workflow publishes")
            return names

    return _retry(request)


def _plan_for_run(
    repo: str,
    run_id: int,
    attempt: int,
    sha: str,
    jobs: list[dict[str, Any]],
    watched: list[str],
) -> dict[str, tuple[str, str]]:
    if not records_target(jobs):
        return failed_verdicts(watched, "validation or target recording did not succeed")
    try:
        _download_record(repo, run_id, attempt, sha, watched)
    except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError) as error:
        return failed_verdicts(watched, f"target record unavailable ({type(error).__name__})")
    return verdicts(jobs, watched)


def _post(repo: str, sha: str, context: str, state: str, reason: str, url: str) -> bool:
    def request() -> bool:
        proc = subprocess.run(
            ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{sha}", "-f", f"state={state}",
             "-f", f"context={context}", "-f", f"target_url={url}", "-f",
             f"description={description(context, reason)}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, proc.args, proc.stdout, proc.stderr)
        return True

    return _retry(request)


def _mark_posted(path: Path, context: str) -> None:
    with path.open("a", encoding="utf-8") as posted:
        posted.write(context + "\n")


def _read_posted(path: Path) -> set[str]:
    try:
        return set(path.read_text(encoding="utf-8").splitlines())
    except FileNotFoundError:
        return set()


def contexts_needing_failure(
    watched: list[str], posted_file: Path, newer_posted: set[str] | None = None
) -> list[str]:
    """Contexts still missing from the active attempt's authoritative verdict."""
    done = _read_posted(posted_file) | (newer_posted or set())
    return [name for name in watched if name not in done]


def _publish_plan(
    repo: str,
    sha: str,
    plan: dict[str, tuple[str, str]],
    url: str,
    posted_file: Path,
    post_status: StatusPost = _post,
) -> list[str]:
    failed = []
    for context, (state, reason) in plan.items():
        try:
            post_status(repo, sha, context, state, reason, url)
            _mark_posted(posted_file, context)
        except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError, RuntimeError):
            failed.append(context)
    return failed


def reconcile_newer_poster(
    repo: str,
    sha: str,
    run_id: int,
    run_attempt: int,
    watched: list[str],
    runs: list[dict[str, Any]],
    fetch_jobs: JobFetch,
    post_status: StatusPost,
    posted_file: Path,
) -> tuple[bool, tuple[int, int] | None]:
    """Re-publish a newer completed run if it appeared while this poster ran."""
    superseded, newer = supersession_decision(
        runs, run_title(sha), run_id, run_attempt, fetch_jobs
    )
    if not superseded:
        return False, None
    newer_run = next(
        run for run in runs
        if int(run["id"]) == newer[0]
        and int(run.get("run_attempt", 1)) == newer[1]
    )
    if newer_run.get("status") != "completed":
        return True, newer
    if set(watched).issubset(_newer_published_contexts(repo, sha, newer_run, watched)):
        return True, newer

    # Earlier statuses no longer represent the newest completed run. Clear the
    # fallback marker before any read or write that can fail.
    posted_file.write_text("", encoding="utf-8")
    jobs = fetch_jobs(newer[0], newer[1])
    failed = _publish_plan(
        repo,
        sha,
        _plan_for_run(repo, newer[0], newer[1], sha, jobs, watched),
        attempt_url(newer_run),
        posted_file,
        post_status,
    )
    if failed:
        raise RuntimeError(f"could not reconcile contexts from newer run {newer[0]}: {', '.join(failed)}")
    return True, newer


def _newer_run_details(
    runs: list[dict[str, Any]], newer: tuple[int, int]
) -> dict[str, Any]:
    return next(
        run for run in runs
        if int(run["id"]) == newer[0]
        and int(run.get("run_attempt", 1)) == newer[1]
    )


def _current_run_is_supported(run: dict[str, Any], repo: str) -> bool:
    head_repo = run.get("head_repository") or {}
    repository = run.get("repository") or {}
    return (
        run.get("event") == "workflow_dispatch"
        and run.get("head_branch") == TARGET_BRANCH
        and head_repo.get("full_name") == repo
        and repository.get("full_name") == repo
        and str(run.get("path", "")).startswith(WORKFLOW_FILE + "@")
    )


def _newer_published_contexts(
    repo: str,
    sha: str,
    run: dict[str, Any],
    watched: list[str],
) -> set[str]:
    source = attempt_url(run)
    latest: dict[str, dict[str, Any]] = {}
    page = 1
    while True:
        batch = _gh_json(
            [f"repos/{repo}/commits/{sha}/statuses?per_page={JOBS_PAGE_SIZE}&page={page}"]
        )
        for context, status in _latest_context_statuses(batch, watched).items():
            latest.setdefault(context, status)
        if set(watched).issubset(latest) or len(batch) < JOBS_PAGE_SIZE:
            break
        page += 1
    return {
        context
        for context, status in latest.items()
        if status.get("target_url") == source
    }


def _fallback(
    repo: str,
    sha: str,
    run: dict[str, Any],
    watched: list[str],
    posted_file: Path,
) -> int:
    run_id = int(run["id"])
    run_attempt = int(run.get("run_attempt", 1))
    fetch_jobs = lambda candidate, attempt: _fetch_jobs(repo, candidate, attempt)
    runs = _dispatch_runs(repo)
    try:
        superseded, newer = supersession_decision(
            runs, run_title(sha), run_id, run_attempt, fetch_jobs
        )
    except UnreadableNewerRun as error:
        # Never let an older fallback overwrite a newer success it cannot read.
        print(str(error), file=sys.stderr)
        return 1

    if superseded:
        newer_run = _newer_run_details(runs, newer)
        if newer_run.get("status") != "completed":
            return 0
        target_url = attempt_url(newer_run)
        try:
            if set(watched).issubset(_newer_published_contexts(repo, sha, newer_run, watched)):
                return 0
            posted_file.write_text("", encoding="utf-8")
            jobs = fetch_jobs(newer[0], newer[1])
            failures = _publish_plan(
                repo,
                sha,
                _plan_for_run(repo, newer[0], newer[1], sha, jobs, watched),
                target_url,
                posted_file,
            )
            if failures:
                raise RuntimeError(f"newer verdict status writes failed: {', '.join(failures)}")
            return 0
        except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError, RuntimeError) as error:
            try:
                already_published = _newer_published_contexts(repo, sha, newer_run, watched)
            except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError):
                print("could not read newer statuses; leaving them untouched", file=sys.stderr)
                return 1
            missing = [name for name in watched if name not in already_published]
            failures = _publish_plan(
                repo,
                sha,
                failed_verdicts(watched=missing, reason="newer any-ref verdict could not be read or posted"),
                target_url,
                posted_file,
            )
            if failures:
                print(f"could not recover newer statuses after {type(error).__name__}: {', '.join(failures)}", file=sys.stderr)
                return 1
            return 1

    missing = contexts_needing_failure(watched, posted_file)
    failures = _publish_plan(
        repo,
        sha,
        failed_verdicts(missing, "any-ref poster failed before publishing this context"),
        attempt_url(run),
        posted_file,
    )
    if failures:
        print(f"could not post fallback failures: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


def _post_run(
    repo: str,
    run: dict[str, Any],
    sha: str,
    watched: list[str],
    posted_file: Path,
    post_status: StatusPost = _post,
) -> int:
    run_id = int(run["id"])
    run_attempt = int(run.get("run_attempt", 1))
    fetch_jobs = lambda candidate, attempt: _fetch_jobs(repo, candidate, attempt)

    superseded, newer = supersession_decision(
        _dispatch_runs(repo), run_title(sha), run_id, run_attempt, fetch_jobs
    )
    if superseded:
        print(f"run {newer[0]} attempt {newer[1]} recorded a target for {sha}; run {run_id} posts nothing")
        return 0

    posted_file.write_text("", encoding="utf-8")
    jobs = _fetch_jobs(repo, run_id, run_attempt)
    plan = _plan_for_run(repo, run_id, run_attempt, sha, jobs, watched)

    failed = _publish_plan(repo, sha, plan, attempt_url(run), posted_file, post_status)
    if failed:
        raise RuntimeError(f"could not post statuses: {', '.join(failed)}")
    reconcile_newer_poster(
        repo,
        sha,
        run_id,
        run_attempt,
        watched,
        _dispatch_runs(repo),
        fetch_jobs,
        post_status,
        posted_file,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("post", "fallback"), required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--posted-file", required=True)
    parser.add_argument("--watch", action="append", required=True)
    args = parser.parse_args(argv)

    try:
        run_attempt = int(args.run_attempt)
        if run_attempt < 1:
            raise ValueError("run attempt must be a positive integer")
        run = _run_details(args.repo, args.run_id, run_attempt)
        if int(run.get("run_attempt", run_attempt)) != run_attempt:
            raise ValueError("workflow run attempt response does not match the requested attempt")
        run.setdefault("html_url", args.run_url)
        if not _current_run_is_supported(run, args.repo):
            print("not a main-dispatched run of this repository's any-ref workflow; nothing posted")
            return 0
        sha = target_from_title(str(run.get("display_title", "")))
        if sha is None:
            print("run title does not contain a canonical any-ref SHA; nothing posted", file=sys.stderr)
            return 1
        if args.mode == "post" and not _is_ancestor(args.repo, sha):
            print(f"{sha} is not on {TARGET_BRANCH}'s history; nothing posted", file=sys.stderr)
            return 2
        posted_file = Path(args.posted_file)
        if args.mode == "fallback":
            return _fallback(args.repo, sha, run, args.watch, posted_file)
        return _post_run(args.repo, run, sha, args.watch, posted_file)
    except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError, RuntimeError) as error:
        print(f"any-ref status publication failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
