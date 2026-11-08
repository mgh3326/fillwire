#!/usr/bin/env python3
"""Publish watched job verdicts for a validated any-ref CI run."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

TARGET_BRANCH = "main"
VALIDATE_JOB = "validate ref"
RECORD_JOB = "record any-ref target"
DESCRIPTION_LIMIT = 140
JOBS_PAGE_SIZE = 100
API_RETRY_ATTEMPTS = 4


class UnreadableNewerRun(RuntimeError):
    """A newer candidate may publish, but its state could not be established."""


def job_matches(job_name: str, watched: str) -> bool:
    """Match a direct job or a job inside a reusable workflow."""
    return job_name == watched or job_name.endswith(" / " + watched)


def run_title(sha: str) -> str:
    return f"any-ref {sha}"


def newer_runs(
    runs: list[dict], title: str, run_id: int, run_attempt: int
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


def records_target(jobs: list[dict]) -> bool:
    """A run supersedes another poster only after validation and record upload."""
    def succeeded(name: str) -> bool:
        found = [job for job in jobs if job.get("name") == name]
        return len(found) == 1 and found[0].get("conclusion") == "success"

    return succeeded(VALIDATE_JOB) and succeeded(RECORD_JOB)


def supersession_decision(
    runs: list[dict],
    title: str,
    run_id: int,
    run_attempt: int,
    fetch_jobs,
) -> tuple[bool, tuple[int, int] | None]:
    """Return whether a newer validated-and-recorded attempt supersedes this one.

    An unreadable newer run is an error, never evidence that this run should
    silently publish or disappear.
    """
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
            f"superseded; failing so it can be retried: {', '.join(unreadable)}"
        )
    return False, None


def verdicts(jobs: list[dict], watched: list[str]) -> dict[str, tuple[str, str]]:
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


def _gh_json(args: list[str]) -> dict:
    def request() -> dict:
        out = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True).stdout
        return json.loads(out)

    return _retry(request)


def _fetch_jobs(repo: str, run_id: str, attempt: str) -> list[dict]:
    jobs: list[dict] = []
    page = 1
    while True:
        batch = _gh_json(
            [f"repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page={JOBS_PAGE_SIZE}&page={page}"]
        ).get("jobs", [])
        jobs.extend(batch)
        if len(batch) < JOBS_PAGE_SIZE:
            return jobs
        page += 1


def _dispatch_runs(repo: str) -> list[dict]:
    runs: list[dict] = []
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


def reconcile_newer_poster(
    repo: str,
    sha: str,
    run_id: int,
    run_attempt: int,
    watched: list[str],
    runs: list[dict],
    fetch_jobs,
    post_status,
    posted_file: Path,
) -> tuple[bool, tuple[int, int] | None]:
    """Re-publish a newer completed run if it appeared while this poster ran."""
    superseded, newer = supersession_decision(
        runs,
        run_title(sha),
        run_id,
        run_attempt,
        fetch_jobs,
    )
    if not superseded:
        return False, None
    newer_run = next(
        run for run in runs
        if int(run["id"]) == newer[0]
        and int(run.get("run_attempt", 1)) == newer[1]
    )
    # An in-progress run has not emitted its own workflow_run completion event
    # yet. Its default-branch poster will publish after its jobs finish.
    if newer_run.get("status") != "completed":
        return True, newer
    jobs = fetch_jobs(newer[0], newer[1])
    if not records_target(jobs):
        return True, newer
    plan = verdicts(jobs, watched)
    target_url = str(newer_run.get("html_url", ""))
    for context, (state, reason) in plan.items():
        if post_status(repo, sha, context, state, reason, target_url):
            _mark_posted(posted_file, context)
        else:
            raise RuntimeError(f"could not reconcile {context} from newer run {newer[0]}")
    return True, newer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--target-url", required=True)
    parser.add_argument("--posted-file", required=True)
    parser.add_argument("--watch", action="append", required=True)
    args = parser.parse_args(argv)

    if not _is_ancestor(args.repo, args.sha):
        print(f"{args.sha} is not on {TARGET_BRANCH}'s history; nothing posted", file=sys.stderr)
        return 2

    try:
        superseded, newer = supersession_decision(
            _dispatch_runs(args.repo), run_title(args.sha), int(args.run_id), int(args.run_attempt),
            lambda run_id, attempt: _fetch_jobs(args.repo, str(run_id), str(attempt)),
        )
    except UnreadableNewerRun as error:
        print(str(error), file=sys.stderr)
        return 1
    if superseded:
        print(f"run {newer[0]} attempt {newer[1]} recorded a target for {args.sha}; run {args.run_id} posts nothing")
        return 0

    posted_file = Path(args.posted_file)
    posted_file.write_text("", encoding="utf-8")
    plan = verdicts(_fetch_jobs(args.repo, args.run_id, args.run_attempt), args.watch)
    failed = []
    for name, (state, reason) in plan.items():
        print(f"{name}: {state} ({reason})")
        try:
            _post(args.repo, args.sha, name, state, reason, args.target_url)
            _mark_posted(posted_file, name)
        except (subprocess.CalledProcessError, OSError, ValueError, TypeError, AttributeError):
            failed.append(name)
    if failed:
        print(f"could not post: {', '.join(failed)}", file=sys.stderr)
        return 1

    try:
        reconcile_newer_poster(
            args.repo,
            args.sha,
            int(args.run_id),
            int(args.run_attempt),
            args.watch,
            _dispatch_runs(args.repo),
            lambda run_id, attempt: _fetch_jobs(args.repo, str(run_id), str(attempt)),
            _post,
            posted_file,
        )
    except UnreadableNewerRun as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
