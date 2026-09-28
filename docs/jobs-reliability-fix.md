# Background job reliability fix

## Goal

Close three failure-handling gaps in the deployed activation release (`e145aec`)
and deliver one review-ready PR. Temporary failures should resolve through
bounded retries of durable work. Duplicate delivery, an ambiguous send, a lost
response, overlapping recovery, and a stopped worker must not publish twice,
consume another daily generation allowance, or alter learner history.

### 1. Retry temporary refill failures

Choice, exact-input and math refills must distinguish temporary provider,
storage, database and stage-timeout failures from rejected content or invalid
configuration. A temporary failure must preserve the same generation job and
enter the existing SQS retry cycle. Persist its retryable checkpoint before
retrying; protect stage and publication writes with the claimed attempt identity.
Retries remain bounded by the delivery and generation limits. Exhaustion must
leave a visible failed job and dead letter. Invalid candidates, failed semantic
verification and policy changes remain terminal and publish no exercises.

### 2. Recover failed or ambiguous publication

Persist publication intent with the domain transaction for refills and every
material ingestion entry point: PDF completion, website import, explicit retry,
PDF reindex and website refresh, including agent PDF completion and URL imports.
Reuse a stable envelope identity and timestamp
through transport retries. A failed send must leave recoverable intent rather
than immediately failing the domain job. The scheduled maintenance sweep must
claim pending publication, retry with a bounded delay, and record terminal
exhaustion. It must handle a crash before send, acceptance followed by a lost
response, a crash after acceptance, concurrent maintainers, and an expired
publisher lease. A stale publisher must not overwrite a newer lease or a
running/completed domain job. Deletion, ownership and material status checks
must prevent canceled or deleted work from being republished.

### 3. Bound work and explain failures safely

Pass the worker delivery deadline into all refill families. Bound source and
database reads, generation, verification and publication; propagate abort to
storage and providers. Database bounds must cancel server-side work rather
than merely abandon a JavaScript promise. Keep time for durable cleanup before
Lambda's hard limit and fence late results. Use current completion times so a
slow attempt does not immediately appear stale. Maintenance and worker logs
must identify approved error codes, SQL states and stages without printing
private documents, raw SQL, credentials or provider bodies.

## Verification

Write failing tests before meaningful behavior changes. Cover transient versus
terminal outcomes in all three refill families; successful retry and exhaustion;
unchanged introductions, attempts, reviews and FSRS; late results and stale
attempts; pending publication after send failure; ambiguous sends; crashes on
both sides of publication; overlapping recovery; deletion and ownership;
publication exhaustion; abort propagation; canceled database reads; and redacted
diagnostics. Reuse existing fixtures and failure-injection helpers.

Run lint, all unit tests, Prisma validation/generation, production build and
worker build. Run database integration tests against a disposable PostgreSQL
database and required browser checks against an isolated test environment.
Inspect remote CI and available automated PR reviews, fix valid findings, and
record the exact verified head and remaining limits. Keep manual review requests
within the repository's two-request limit.

Production deployment, production failure injection, database plan upgrades,
and replacing SQS are outside this implementation. No production learner data
will be submitted or mutated by tests. The PR must explain the required web and
worker rollout, compatibility with older publication markers, and rollback.

## Status

Implemented on `a/jobs-reliable-recovery`. The fix uses the existing generation
job and material revision rows as durable publication intent. It adds no table,
migration, service, queue, scheduler or provider. Production has not changed.

## Recovery contract

| Failure | What happens |
| --- | --- |
| Temporary refill provider, source or database failure | Persist a retry checkpoint and throw a typed error to the worker. Replay the same generation job, with at most three executions and no second daily allowance. |
| Invalid content, failed semantic verification or authentication failure | Publish no exercises; preserve a terminal domain failure. |
| Process exits after the domain commit but before queue publication | Maintenance finds the persisted intent and sends it. |
| Queue accepts a send but its response is lost | Retry the stored envelope identity and payload. The durable delivery ledger and domain claims tolerate duplicates beyond SQS's deduplication window. |
| Worker starts or finishes before the publisher hears back | Publisher completion/failure updates compare the lease and domain state. They cannot overwrite the worker. |
| Two publishers overlap, or an old publisher wakes after its lease expires | Only the current compare-and-set lease can finish. Preserve unrelated diagnostic metadata. |
| Five publication attempts fail | Leave a visible failed domain record and a safe diagnostic. Explicit user repair starts a new publication cycle. A genuinely accepted late refill delivery can still claim an unexecuted job. |
| Worker is killed | Initial SQS visibility remains 3,600 seconds. After a durable claim, visibility becomes 720 seconds, beyond the 660-second ledger lease and 600-second Lambda limit. A stuck visibility request aborts after five seconds; no business work starts if it fails. |
| Last refill execution is killed, or terminal delivery cannot be acknowledged | Finalize unfinished refill state before sending the dead letter. If finalization or DLQ publication fails, leave the message available for another bounded transport attempt. Preserve completed and semantic outcomes. |
| Provider returns after cancellation, or an older attempt tries to publish | Abort signals and attempt-fenced stage/publication writes prevent late publication. |
| Account deletion or material deletion is requested | Recovery skips canceled owners/materials. Refill publication locks the account in deletion's lock order and checks its independent tombstone before writing exercises. |

Maintenance recovers up to 25 rows per family after a five-minute quiet period.
Publication attempts are five minutes apart. It stops claiming rows when less
than 45 seconds of delivery time remain. Refills keep 30 seconds for cleanup;
database evidence reads use server-side statement cancellation. Logs carry only
approved codes, HTTP status numbers, stage names and identifiers, never raw
provider bodies, SQL, credentials or study text.

## Verification evidence

Failure-injection tests were first observed failing, then passed after the
corresponding changes. The complete local database gate passed **623 tests in
49 suites** against disposable PostgreSQL 18 with pgvector and all migrations.
It covers all three refill families, successful replay, bounded exhaustion,
lost acknowledgements, crashes, overlapping publishers, stale attempts,
deletion, ownership, malformed intent and unchanged learner history.
Agent import tests cover PDF/URL publication recovery, stable identity after a
lost acknowledgement, and operation polling after bounded send exhaustion.
Successful choice refill retries record their actual attempt and retry count.

Local lint, TypeScript, Prisma validation/generation, the web production build
and Lambda package build passed. The unit gate passed **1,411 tests in 154 suites**.
Anonymous browser checks passed **15 tests** at desktop and mobile widths.
Check remote CI and review findings on this branch's PR before merge; those
results are attached to its exact commit and must not be inferred from local tests.
Integration tests now disable locally inherited
Meta fallback credentials so mocked provider failures cannot make live AI calls.

## Rollout and rollback

1. Merge only after the required PR checks and review findings are resolved.
2. Build/deploy the merged worker to staging using
   [the existing worker deployment procedure](aws-background-jobs.md#deploy),
   with staging schedules disabled. Verify the deployed artifact hash and run
   an isolated refill/import canary without introducing skills or recording
   learner attempts. Exercise temporary failure/recovery in staging.
3. Release the merged worker to production with its existing schedules enabled,
   then deploy/verify the web release. Both artifacts are required: a web deploy
   alone does not update Lambda. Worker-first deployment can read older refill
   delivery markers, including ones without a publisher lease ID.
4. Verify queue/DLQ depth, errors, maintenance publication counters and pending
   domain rows. Refill execution exhaustion reaches the existing DLQ; publication
   exhaustion is a visible domain failure and safe log, requiring explicit repair.

No schema migration is required. Old material revisions without a durable
publication marker are deliberately excluded from automatic recovery; explicit
retry creates the new marker. The fix does not blindly replay historic dead
letters. If rollback is necessary, retain publication metadata and delivery
records, restore the previously verified worker/web artifacts, and inspect
pending rows before repair. An older worker does not provide these new recovery
guarantees, so rolling it back pauses automatic material publication recovery.

This is verified failure handling, not a promise that every outage will heal
without intervention. Provider authentication errors, persistent outages,
rejected content and unavailable database infrastructure can still reach
bounded failure. Production failure injection, failover and backup restoration
were not performed as part of this PR.
