# Any-ref CI

`ci-any-ref.yml` lets mergewake test an exact commit on `main` when that
commit's push run was cancelled by a newer push. Dispatch it with the required
`ref` input set to a lowercase 40-character commit SHA. The workflow checks that
the SHA is on `main`, then runs the existing `verify` and `secrets` checks at
that commit.

The dispatch workflow has read-only permissions. It cannot publish an image,
post `ci-ok`, or invoke `deploy.yml`. After completion,
`post-ci-any-ref-status.yml` runs from the default branch and posts `verify`
and `secrets` statuses on the recorded SHA. Each status is green only when one
matching job completed successfully; missing, duplicate, skipped, cancelled,
and failed jobs produce failure statuses. The poster derives the target from
the dispatch run title as well as the uploaded record, so it can report failure
if validation succeeded but recording failed. It is one job, which keeps
recovery available even when target resolution fails. A newer run supersedes
an older poster only after both validation and target recording succeeded.
Poster runs use distinct concurrency groups, retry transient GitHub API calls,
reconcile a newer completed run that appears during publication, preserve
statuses already published by that newer run, and attempt failure statuses for
watched contexts it could not write. The verdict tests run in CI with the
existing Python checks.
