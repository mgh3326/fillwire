#!/usr/bin/env python3
"""Publish watched job verdicts for a validated any-ref CI run."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

TARGET_BRANCH = "main"
VALIDATE_JOB = "validate ref"
RECORD_JOB = "record any-ref target"
DESCRIPTION_LIMIT = 140
JOBS_PAGE_SIZE = 100


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


def _gh_json(args: list[str]) -> dict:
    out = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


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
    proc = subprocess.run(
        ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{sha}", "-f", f"state={state}",
         "-f", f"context={context}", "-f", f"target_url={url}", "-f",
         f"description={description(context, reason)}"],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--target-url", required=True)
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

    plan = verdicts(_fetch_jobs(args.repo, args.run_id, args.run_attempt), args.watch)
    failed = []
    for name, (state, reason) in plan.items():
        print(f"{name}: {state} ({reason})")
        if not _post(args.repo, args.sha, name, state, reason, args.target_url):
            failed.append(name)
    if failed:
        print(f"could not post: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
