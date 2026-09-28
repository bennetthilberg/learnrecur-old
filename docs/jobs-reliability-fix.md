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
PDF reindex and website refresh. Reuse a stable envelope identity and timestamp
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

Implementation started on `a/jobs-reliable-recovery`. No runtime changes or new
production verification have occurred yet. Update this section at completion
with commits, tests, review state and rollout instructions.
