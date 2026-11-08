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
the dispatch run title and requires the uploaded record to name that exact SHA.
It reads the attempt named by `workflow_run` and downloads that attempt's
uniquely named target record.

A successful verdict is posted only while its target remains on `main`'s
history. If that check fails, the failure fallback still posts failure statuses
for the watched contexts. The poster is one job, which keeps recovery available
even when validation or target recording fails. A newer run supersedes an older
poster only after both validation and target recording succeeded. Poster jobs
for the same run title (candidate SHA) use one concurrency group and do not
cancel the active poster. The helper compares the newest status per context,
because older statuses remain in GitHub's status history. It retries transient
GitHub API calls, reconciles a newer completed run that appears during
publication, and attempts failure statuses for watched contexts it could not
write. The verdict tests run in CI with the existing Python checks.
