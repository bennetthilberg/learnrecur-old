# Spanish import readiness status

Updated 2026-09-26 for the activation latency investigation. This is a
work-in-progress checkpoint, not a release receipt.

The implementation provides the bounded, auditable plumbing for a future
Spanish textbook pilot: sequential material reading, revision-bound source
references, resumable batch operations, full exercise audit/history reads, and
issue adjudication with cumulative evidence correction. The repository does
not contain a licensed Spanish textbook, so this record does not claim live
book coverage, generated-exercise quality, or a completed representative
material pilot.

The adopted capacity boundary is 250 active or paused skills. Import capacity
is separate from the learner's daily new-skill introduction setting. Import
operations do not set `firstIntroducedAt`, create attempts, advance FSRS, or
consume that learner allowance. Pausing does not free a library slot;
archiving does.

The required live follow-up is a representative real-material pilot using the
ledger in [the textbook import playbook](textbook-import-playbook.md): traverse
at least one chapter, preserve source locators, audit generated answer
contracts, exercise the issue correction flow, and record extraction gaps.

## Development checkpoint

PR #146 merged to `main` at `da807e6`. The implementation checkpoint `fe7a1da`
was followed by review-fix commits `6dd1a10`, `7fe1f63`, `49c99d9`, `e340a17`,
`3cdb901`, `968f782`, `7514e5a`, and `8ed83bf`. The unrelated untracked
`docs/product-discovery/` directory belongs to the user and must remain
untouched.

### Production schema incident

After PR #146 deployed, a newly authenticated production account reached the
dashboard and history routes but both pages failed at their route error
boundary. Vercel runtime logs identified Prisma `P2022` errors for missing
`exercise_attempts.evidenceCorrectionStatus` and
`review_logs.evidenceCorrectionStatus` columns. OAuth account creation itself
succeeded.

The production and staging migration ledgers had stopped at
`20260908120000_advanced_practice_settings_and_sessions`; CI had applied the new
migrations only to its disposable database. On 2026-09-18, production backup
`b004` completed before repair. The following migrations were then applied and
read back successfully in staging and production:

- `20260917170000_quality_incident_replay_corrections`;
- `20260918040000_agent_source_reference_outcomes`.

The repair also confirmed the two correction columns and
`agent_skill_operation_items.sourceReferenceOutcome`. A production log query
afterward found no new matching `P2022` correction-column errors. The permanent
release fix completes the fallible, tier-aware prebuild before
production-target Vercel builds run tracked migrations, invokes Next directly
afterward, uses verified TLS for remote migration connections, and keeps
preview/development builds away from hosted migrations.

Implemented in the current working tree:

- shared import limits and account-locked reservation paths for native and
  connected-agent work;
- bounded per-delivery processing with durable operation-item continuation;
- sequential section/PDF-page material reading with opaque cursors, snapshots,
  extraction-gap reporting, and no study-state mutation;
- revision-bound, ownership-checked source references and canonical PDF/web
  locators;
- answer-bearing exercise audit reads, completed practice history, and
  consented MCP tools for audit, history, issue listing, and issue resolution;
- cumulative confirmed-defect correction, historical annotations, replay-safe
  scheduling, practice-only preservation, idempotency/stale guards, and
  separate replacement-queue outcomes;
- duplicate reuse attaches compatible new source references and reports whether
  evidence was attached, merged, or preserved on the operation item;
- issue-list responses keep submitted answers behind the separate
  `practice:history` consent, with the settings surface naming all three new
  exercise-audit/history/resolution permissions;
- focused Needs Attention and issue-resolution feedback in the learner UI;
- import playbook and a small import ledger template.

Verification evidence for the current local checkpoint:

- `npx tsc --noEmit`, `npm run lint`, and the focused audit tests passed after
  `8ed83bf`;
- the full unit suite passed 142 files and 1,225 tests after `8ed83bf`;
- the latest remote verify job passed lint, unit tests, Prisma validation and
  generation, runtime audit, and the production build;
- `npm run prisma:validate`, `npm run prisma:generate`, `npm run jobs:build`,
  and `npm run check:runtime-audit` passed; the runtime audit reports only
  three already-accepted dev-only Prisma CLI exceptions;
- anonymous Playwright passed all 15 browser checks at desktop and mobile
  widths using an isolated local port; an initial run had reused an unrelated
  Spanish-grammar dev server already occupying port 3000 and is not product
  evidence;
- the focused refill/reservation suite passed 34 tests;
- database integration remains an environment limitation: the configured Neon
  branch is read-only (`25006`) for temporary test writes and the bounded full
  run cannot complete there. The remote isolated database run is authoritative;
- remote CI run `35307403131` passed both the verify job and the authenticated
  integration/coverage and browser job;
- Vercel production deployed successfully at
  `https://vercel.com/learn-recur/learnrecur/2FZvuoq3WmUnTTU3gvcYLmgHy55g`,
  and agent staging deployed successfully at
  `https://vercel.com/learn-recur/learnrecur-agent-staging/4dujVdhJEF1urvGUaWaD4TAcxSjP`;
- the two current Codex review threads were answered and resolved in `8ed83bf`;
  the prior CodeRabbit actionable threads are all resolved or outdated, and
  the current PR has no unresolved non-outdated review thread. CodeRabbit's
  status context remains pending with “Review in progress,” so it is not being
  represented as a clean automated review pass;
- no live WorkOS registration/reconsent, licensed textbook, or representative
  material pilot has been performed for this branch.

The Spanish import implementation is merged. The database limitation must
remain recorded as an environment limitation rather than a passing result. The
next operator should run the real-material pilot before making coverage or
exercise-quality claims.

## Worker deadline blocker follow-up — 2026-09-23

The production handoff at 20:05 UTC reported 38 active skills, 50 pending
operation items, and no activation progress after 19:47 UTC. The handoff's FIFO
head-of-line explanation is an inference from the worker timing and live
symptoms. AWS queue receipts and CloudWatch logs were not available because the
local AWS session had expired, so this branch does not claim a confirmed queue
diagnosis or a production fix.

The fix is under review in PR #154 on `a/activation-worker-deadline` and is not
deployed to production:

- Agent operation deliveries now have an eight-minute soft deadline, below
  Lambda's ten-minute hard timeout, with reserved cleanup time.
- Skill generation, exercise verification, embedding, material drafting, and
  activation publication use bounded stages. Provider calls receive abort
  signals; database publication transactions have explicit wait and execution
  limits.
- Timed-out work releases its claim and persists a bounded transient retry.
  Eligible work can continue in a fresh delivery; maintenance republishes
  queued work with a stable event ID after ambiguous sends.
- Maintenance filters retry eligibility before applying its bounded scan, so
  delayed retry rows and upload-waiting operations cannot hide ready work
  behind the scan limit.
- Continuation publish timeouts and failures now reject the current job
  delivery for retry; ambiguous publication remains safe through stable event
  IDs and item claim fencing.
- Maintenance rethrows `JobContinuationLimitError` from refill and activation
  recovery so the worker records the safety-limit breach as a permanent failure
  and sends it to the FIFO dead-letter queue.
- Background material planning preserves an idempotent `PLANNING` batch for a
  retryable worker timeout. Synchronous plan and replan actions return a normal
  failed result instead of throwing through the action boundary.
- Lazy PDF OCR, material chunk retrieval and embeddings, and final skill
  similarity checks also have deadline-bound stages. Their S3 and AI calls
  receive abort signals; canceled OCR releases its page claims for retry.
- Scope planning and its optional review now receive the remaining delivery
  deadline and abort provider work before the worker cleanup margin. Material
  summaries have an explicit provider timeout instead of inheriting the shorter
  generic Gemini timeout.
- The FIFO worker intentionally publishes bounded follow-ups to its own queue.
  Its CloudFormation setting explicitly allows this documented pattern; each
  delivery can publish at most 100 child jobs, each envelope carries a validated
  continuation depth capped at 64, and Lambda reserved concurrency matches the
  event-source cap. A breached safety limit is terminal and visible in the
  worker log and FIFO dead-letter queue, which is alarmed in both environments.
- Publication checks preserve verified candidates and use transaction fences
  to avoid duplicate skill activation. The worker does not alter introduction
  timestamps, attempts, or FSRS history.

Local verification for this branch:

- `npm run test:unit`: 148 files and 1,284 tests passed after the final review fixes;
- `npx tsc --noEmit`, `npm run lint`, and `npm run prisma:validate` passed;
- `npm run prisma:generate`, `npm run jobs:build`, and `npm run build` passed;
- the full database baseline passed all 46 files and 561 tests against a fresh
  temporary local PostgreSQL 18 database with `pgvector`; the tracked
  migrations applied successfully. After the final review fixes, the focused
  `agent-access` and `material-drafting` suites passed 95 tests total. The
  configured Neon branch remains read-only (`25006`);
- the integration suite covers never-resolving verifier, planner, reviewer,
  OCR, retrieval, back-matter recovery, and similarity stages; sibling progress;
  activation-publication timeout; preservation of verified candidates; stable
  recovery event IDs; upload-expiry isolation; fairness when upload-waiting rows
  exceed the continuation scan cap; retry after continuation publication
  failure; synchronous planning timeouts; and retryable OCR claim release when
  its provider is aborted;
- final CodeRabbit follow-up suites passed 102 tests across `agent-access`,
  `material-drafting`, and `material-evidence` on a fresh local PostgreSQL 18
  database, plus 16 focused unit tests across material embeddings, evidence,
  S3 reads, and job deadlines. The configured Neon test branch rejected writes
  as read-only; no test rows were written there;
- `npx tsc --noEmit` and ESLint on all changed code and tests passed after the
  final review fixes;
- AWS access was restored on 2026-09-24 and used for read-only inspection.
  The live worker, recursion metric, alarm, and queue evidence is recorded
  below. Production deployment, duplicate-activation checks under a live mixed
  batch, and the requested production smoke acceptance remain unverified.

### AWS recursive Lambda notification

AWS sent a recursive-invocation termination alert for the jobs worker around
2026-09-24 00:16 UTC. The alert says Lambda stopped the detected invocation
chain. Read-only CloudWatch inspection on 2026-09-24 confirmed three
`RecursiveInvocationsDropped` datapoints, labeled 00:10, 01:10, and 02:10 UTC.
The first aligns with the email at 00:16 UTC; the next two show the drops
continued hourly after the notification. Across the queried 00:00–04:00 UTC
window, all 30 Lambda `Errors` datapoints were zero. Lambda's recursion-drop
metric is separate from the ordinary error metric.

The production worker was still the pre-PR deployment at inspection: active,
last modified 2026-09-23 08:20 UTC, with a 600-second timeout. Its `RecursiveLoop`
and reserved-concurrency fields were unset, and its enabled FIFO event-source
mapping used batch size 1 with maximum concurrency 5. The main queue had 1
visible and 1 in-flight message; its FIFO dead-letter queue had 3 visible
messages. The production `QueueAge` and `DeadLetterBacklog` alarms were both in
`ALARM`: queue age crossed 900 seconds at 21:23 UTC on 2026-09-23, and the
dead-letter alarm's last breaching datapoint was 1 at 04:31 UTC on 2026-09-18.
The latest `ApproximateAgeOfOldestMessage` datapoint was 19,511 seconds (about
5 hours 25 minutes) at 02:30 UTC, up from 4,631 seconds at 22:00 UTC. From
00:00–02:30 UTC, receive and delete metrics remained around one or two messages
per five-minute period while the oldest-message age continued to rise. The
queue visibility timeout is 3,600 seconds, retention is four days, and its
redrive policy allows six receives. The hourly recursion
drop cadence matches the visibility interval, so repeated delivery of an old
message is plausible, but current metrics and logs do not prove it is the same
message or identify its job.

Repository code confirms that worker handlers can publish follow-up messages
directly to their own FIFO-triggered queue, so a sufficiently long valid
fan-out can reach AWS's recursion threshold. Safe worker logs sampled around
each drop show successful maintenance runs at 00:08, 01:08, and 02:08 UTC, each
reporting zero activation continuations; account-deletion recovery also
completed in those windows. These entries do not establish which invocation
started the recursive chain. The metric confirms three drops, but not the
originating job. No SQS messages were received or changed during this
inspection, and no production configuration was modified. Reading a message
with `ReceiveMessage` would temporarily change its visibility and increase its
receive count, so message bodies and exact job identity remain uninspected.

PR #154 explicitly allows the intentional SQS/Lambda chain and adds the
bounded follow-up, continuation-depth, and reserved-concurrency guardrails
described above. This is a code/template fix only; it is not deployed. The
current queue and DLQ backlog need review before any redrive, and the mixed-batch
acceptance check remains pending the reviewed release path.

After PR CI and review, the operator should deploy through the normal release
path, run the small mixed batch from the handoff, and verify sibling progress,
terminal or bounded-retry outcomes, no duplicate activations, verified exercise
inventory, and unchanged introduction/FSRS history before resuming imports.

Suggested resume commands:

```bash
cd /Users/main/repos/learnrecur
git switch main
git pull --ff-only
git status --short --branch
cat docs/spanish-import-readiness-status.md
rg -n "source_refs|recent_attempts|AGENT_ACCESS_SCOPES|permissionSummary" src tests docs
```

### Live worker snapshot — 2026-09-24 04:36 UTC

Further read-only AWS inspection confirmed the alert and queue backlog were
still active:

- The production Lambda remained on the pre-PR version (last modified
  2026-09-23 08:20 UTC), with recursion set to `Terminate`, a 600-second
  timeout, and no reserved concurrency. Its enabled SQS mapping used batch size
  1 and maximum concurrency 5.
- The main queue had 1 visible and 2 in-flight messages; the FIFO DLQ had 4
  visible messages. The latest queue-age datapoint was 4,411 seconds at 04:25
  UTC. Both `QueueAge` and `DeadLetterBacklog` alarms remained in `ALARM`.
- `RecursiveInvocationsDropped` datapoints timestamped 00:00, 01:00, 02:00,
  03:00, and 04:00 UTC had sums of 1, 1, 1, 2, and 2 respectively. The 04:00
  datapoint belongs to an incomplete hour at inspection time. Drops continued
  after the AWS email.
- CloudWatch Logs Insights showed 55 successful agent-access maintenance runs
  from 00:03 through 04:33 UTC. Every run reported zero activation items
  requeued and zero activation continuations. No structured non-completion job
  outcomes appeared in that window. Three agent-skill-operation deliveries
  completed between 03:13 and 03:15 UTC; none completed afterward in the queried
  interval. These records do not identify which message triggered recursion.
  No structured non-completion outcome was logged in the queried window.
- The LearnRecur progress summary at 04:31 UTC showed 73 active skills, 12
  pending operation items, zero introductions, and zero reviews. Preparation
  reported 48 failed jobs and 2 pending jobs.

I did not receive, change, or redrive any SQS messages. The connected AWS CLI
credentials were active during this inspection. The exact queued job remains
unidentified because receiving a message would change its receive count and
visibility, and no production configuration change has been applied.

At this snapshot, PR #154 was open at code revision `4601008` with
`CHANGES_REQUESTED`. Its unresolved CodeRabbit Major finding concerns OCR claim cleanup after timeout
([review thread](https://github.com/bennetthilberg/learnrecur/pull/154#discussion_r4089646646)).
The three permitted review-response cycles have been used; do not deploy this
branch until the remaining data-integrity issue is resolved and verified. CI
`verify`, `authenticated-e2e`, and Vercel previews passed on code revision
`4601008`, but those checks do not establish the production mixed-batch
acceptance criteria.

### Timeout review follow-up — 2026-09-24 14:41 UTC

The current working tree resolves the remaining OCR review finding. OCR timeout
handling now gives canceled work at most five seconds to finish cleanup, and
material planning treats a fresh OCR claim in its selected page range as a
retryable in-progress state. Both checks and claims run under the material lock,
so another planner cannot silently skip a live claim and save a partial plan.
The readiness resume command now prints the entire document.

Local verification passed: `npm run lint`, `npm run test:unit` (148 files,
1,287 tests), `npm run prisma:validate`, `npm run prisma:generate`,
`npx tsc --noEmit`, and `npm run build`. The new database regression tests could
not run locally: the configured database connection rejected fixture inserts
as read-only before any test data was created. Run them in the write-enabled CI
database after pushing.

At this checkpoint, the patch is still uncommitted on
`a/activation-worker-deadline`; PR #154 still points to `e64a18c`. AWS CLI
authentication has expired, so I could not refresh the live Lambda, queue, or
alarm snapshot. No SQS messages or production configuration were changed. The
04:36 UTC snapshot above remains the last verified AWS state; restore CLI access
before production acceptance or any release action.

### Continuation-limit review follow-up — 2026-09-24 14:59 UTC

PR #154 is now at `c9624a7`; its `verify` workflow passed, including the
write-enabled database integration suite. Authenticated browser tests and the
Vercel production preview were still running at this checkpoint. A fresh local
CodeRabbit review found that `runAgentSkillOperationJob` could wrap
`JobContinuationLimitError` as retryable. The working-tree fix now reconciles
the operation and rethrows that typed permanent error, with a database
regression test for a rejected continuation at the safety limit. TypeScript,
lint, and all 1,287 unit tests pass for this follow-up. This second patch is not
yet committed or pushed; no production changes have been made.

### Full review and hosted CI checkpoint — 2026-09-24 15:24 UTC

PR #154 is at `c431f45`. Hosted `verify` and `authenticated-e2e` passed on this
revision, including the write-enabled database integration suite and
authenticated browser tests. Both Vercel previews also deployed successfully.
The full committed CodeRabbit CLI review covered all 50 changed files and
reported one trivial test-hardening issue: the MetaMuse cancellation test
needed to assert that its mocked Gemini request had started before aborting.
That assertion is now added, and `npx vitest run tests/unit/material-ai.test.ts`
passes (7 tests). The assertion and this checkpoint are not yet committed.

GitHub currently reports 17 review threads with none unresolved. The PR
decision still reads `CHANGES_REQUESTED`; CodeRabbit's PR check is paused. The
manual Codex review request limit has already been used, so no additional manual
`@codex review` request is available. The local full-diff CodeRabbit review is
clean apart from the test assertion fixed above.

AWS credentials remain expired: a read-only `aws sts get-caller-identity`
check at 15:11 UTC returned `Your session has expired`. No `aws login`, queue
read, message receive/redrive, deployment, or production configuration change
has occurred since the 04:36 UTC snapshot. The current production Lambda,
backlog, alarms, and recursive-drop metric therefore remain unverified. The
mixed-batch acceptance check and import resume are still blocked on live AWS
access and the reviewed release path; learner state has not been touched.

### PR and live AWS checkpoint — 2026-09-24 15:58 UTC

PR #154 is open at `b03f84c` on `a/activation-worker-deadline`. Hosted CI run
`36022085794` passed `verify` and `authenticated-e2e`, including the write-enabled
database integration suite with coverage and authenticated browser tests. Both
Vercel previews passed. Local lint, all 1,287 unit tests, and TypeScript
typechecking also passed at this code revision.

The latest CodeRabbit Major finding about OCR claim cleanup is fixed in
`b03f84c`. CodeRabbit acknowledged the fix and marked that thread resolved; the
regression ran in the hosted database suite. All 18 PR review threads are
resolved. GitHub's aggregate review decision still says `CHANGES_REQUESTED`,
and the CodeRabbit check says `Review paused`; do not describe the current head
as approved or freshly reviewed. The manual Codex review-request limit was
already exhausted on this PR.

The renewed AWS CLI session verified account `168992393637`. The AWS Health
console shows the account-specific `Lambda runaway termination notification`
event beginning `2026-09-24T00:16:14Z`
(`AWS_LAMBDA_RUNAWAY_TERMINATION_NOTIFICATION-024ac3d7-dd40-493e-94c6-d27deded6e79`).
Its sole affected resource is `learnrecur-production-jobs-worker`. The Health
API returns `SubscriptionRequiredException` for this account, so the console is
the source for the affected-resource detail.

The live Lambda is still the pre-fix artifact: active, 600-second timeout,
last modified `2026-09-23T08:20:20Z`, with code hash
`4hR1miz1qRqSmEhAaDjU84PYH9HA4y6f69pBdSBpwZM=`. The `RecursiveLoop` field is
not set in its live configuration, and it has no reserved concurrency. Its
enabled FIFO SQS mapping has batch size 1 and maximum concurrency 5. At 15:47
UTC, the main queue had 0 visible and 0 in-flight messages; its jobs DLQ had 7
visible messages; the scheduler DLQ had 0. `RecursiveInvocationsDropped`
remained nonzero once or twice per hour through the 09:00 CDT datapoint.
CloudWatch showed `QueueAge` `OK` and `DeadLetterBacklog` `ALARM`.

No production configuration was changed, no SQS messages were received or
redriven, and no learner state was modified. The PR has not been deployed, so
the handoff's production mixed-batch acceptance and Spanish import resume are
still pending the reviewed release path. Poll existing operation IDs before
resuming; preserve zero new introductions/reviews during that canary, and do
not redrive the 7 DLQ messages without identifying and reviewing them.

### Post-merge release checkpoint — 2026-09-24 17:20 UTC

PR #154 merged as `3818819`. GitHub Actions `ci` passed on that commit, but the
Vercel production build failed while resolving `next/font/google` through
Turbopack. The agent-staging Vercel build of the same commit passed. A Vercel
production redeploy without build cache compiled successfully, became Ready,
and was aliased to `alpha.learnrecur.com`; the live site returned HTTP 200 and
the merge commit's Vercel status changed to success. No application code change
was needed for that build failure.

The first AWS worker deployment of `3818819` rolled back: the template tried
to reserve five Lambda concurrency slots, while the AWS account's concurrency
quota is ten and AWS requires ten slots to stay unreserved. This follow-up
changes the template so reservation is optional and disabled by default. The
SQS event-source mapping still caps worker concurrency at five. The successful
stack update reused configuration revision
`bb2607c5-bfa1-43f8-a8a6-878a70d2e02e`, kept schedules enabled, and
completed at 17:19 UTC. The live Lambda code hash
`UeCztfe2H9hK9H52+l4DRx6rGJW4Tu6S9xlZbLJKuGg=` matches the package built
from merge commit `3818819`; its recursion configuration is `Allow`, and the
SQS mapping is enabled with batch size one and maximum concurrency five.

At the 17:20 UTC snapshot, the main FIFO queue had zero visible and one
in-flight message. The existing jobs DLQ still had seven visible messages; it
was not redriven. Shared unreserved account concurrency can still throttle
the worker if other Lambda functions consume the quota. Queue age and dead
letter alarms remain the backstop. The production mixed-batch acceptance and
Spanish import resume remain pending; preserve zero introductions and reviews
during that canary and inspect the in-flight operation before resuming it.

### Worker and Muse checkpoint — 2026-09-25 03:28 UTC

The Vercel production deployment serves PR #162's merge commit `24d92b1`, but
the Lambda worker initially still ran the earlier package. A deployment from
current `main` first rolled back because its template again required five
reserved concurrency slots under the account's ten-slot quota. Reusing the
deployed template with `ReserveWorkerConcurrency=false` and changing only the
content-addressed code key succeeded. CloudFormation is `UPDATE_COMPLETE`;
the live Lambda code hash `Pn8lRgJNJv4jYwdcAaiy6ibplXv32wLjKgx0WgG31VY=`
matches the package built from `24d92b1`. Configuration revision and enabled
schedules were preserved. PR #155 carries the optional-reservation fix for
future deployments and is being refreshed against current `main`.

The live provider-handoff smoke forced a synthetic Gemini 503 and exercised
real Muse generation and independent verification. Muse generated five choice
exercises, four passed verification, and the contradictory control was
rejected. The live handoff passed. The evaluation command's overall verdict
remained `pause` because its offline sample contains 17 runs against a
30-run release threshold. This smoke does not prove full textbook import
quality or sustained fallback capacity.

The production worker is still **not healthy**: QueueAge and DeadLetterBacklog
are in `ALARM`, the main FIFO queue had three visible and one in-flight
message after the release, and the jobs DLQ held eight. One delivery logged
`JOB_DELIVERY_FAILED` near the top of each hour from 00:00 through 03:00 UTC.
The current log omits message identity and failure phase, while the production
delivery table shows no current non-completed row. The eight DLQ messages were
inspected without deletion or redrive: three are agent-skill operations, two
are maintenance, two are exercise refills, and one is a due reminder. Several
were native SQS redrives without an application failure code. Their side
effects must be checked individually before any replay.

PR #155 now includes redacted failure diagnostics with validated job identity,
phase, and allowlisted infrastructure error code. Focused tests, lint, and
TypeScript checks passed locally. Once that exact worker artifact is reviewed,
deployed, and hash-verified, observe the next hourly failure to identify the
blocked envelope and underlying phase. Resolve the cause before attempting
the original handoff's production mixed-batch canary or submitting the 14
remaining Spanish specs. No learner introductions or reviews were made here.

### Full-source Muse retrieval check — 2026-09-25 04:29 UTC

The production Spanish textbook revision contains 279 stored chunks and
917,364 source characters. A live Muse scan using the existing 180,000-character
group limit failed because one request did not score every chunk. The scanner
rejected the incomplete result, so no partial evidence was used. After reducing
the group limit to 90,000 characters, the same real-source scan scored all 279
chunks in 55.96 seconds within the 70-second retrieval budget and returned 48
ranked matches. No learner records changed. This proves one complete live scan,
not sustained throughput or exercise quality; the latter still requires the
original handoff's audit.

The 04:00 UTC worker diagnostic identified the blocked midnight due-reminder
envelope during its database claim. It returned after 5.005 seconds, matching
the Postgres connection timeout. A later read-only database check found no
durable delivery row for that envelope and connected normally. The worker
retry, SQS redrive, and Muse scan-limit changes in this PR remain undeployed
at this checkpoint.

### Activation latency follow-up — 2026-09-26

The nine-skill operation `cmuitzv1x000n04l9jywkid48` completed with nine active
skills. Its slow item `cmuitzv2b000p04l9surlfbcn` was stranded after an early
worker failure and recovered by maintenance at 20:33:14 UTC. The first
generation job `cmuiu0aip000202jwnhlnz9cw` remained at checkpoint `claimed`
until recovery marked it `FAILED`/`TIMEOUT`. The replacement job
`cmuiuk9ne000002jtdkmya0d3` succeeded. Read-only production checks found
the skill active with seven verified exercises and no introduction or review.
Do not submit this item again.

CloudWatch retained the delivery sequence. Four deliveries of the first event
ran between 20:17 and 20:22 UTC. The first three logged
`JOB_EXECUTION_FAILED` after short invocations, while the fourth completed.
Maintenance at 20:18 also failed three times; the 20:23 and 20:28 scans found
no stale item. The 20:33 scan requeued the item. No ten-minute Lambda invocation
appears for this item. The logs do not include the underlying exception, so
the exact cause of the first failure remains unverified. The observed failure
mode is an exception after generation-job claim that left a `RUNNING` job behind.
The deployed worker had a 600-second timeout and enabled SQS mapping and
schedules. Its Lambda `CodeSha256` was
`wVbj5dYHQcfLEi9GSh1cq2uwkPmHWJpVB+4HLkR/FMk=`; the local jobs package did
not match that artifact, so source-to-artifact equivalence is unverified.
The deployed maintenance schedule is enabled at `rate(5 minutes)` with no
flexible window. Five-minute SQS metric buckets from 20:15–20:40 UTC put the
main queue's oldest-message maximum at 249 seconds and the jobs DLQ visible
count between one and two. Those queue-wide metrics cannot identify this
item's FIFO group or explain its first worker exception.

The current branch adds 30-second source-evidence and 15-second recent-history
deadlines before the existing generation and verification limits. Stage logs
record safe IDs, duration, remaining deadline, and failure cause. A worker
exception after claim now conditionally fails only its own generation job,
then releases the item for bounded retry. Recovery keeps the claim-token and
generation-job fences and waits longer than the 600-second live Lambda before
reclaiming an apparently dead worker. MCP item progress now says whether an
item is working, waiting for retry, needs action, ready, or stopped. Operational
job and operation completion timestamps use wall-clock time; scheduling still
uses the effective `now` value. The old replacement job's `completedAt` was
10.3 seconds before its `createdAt`, consistent with the prior captured-time
write.

The automatic PR review found two adjacent gaps. Material-batch activation
now passes its worker deadline into the same activation stages. A PDF OCR
claim is released when its stage aborts, even if the underlying generator
ignores cancellation; a claim still held by another worker is reported as
retryable pending evidence instead of a permanent missing-source error.
Recent-history and material-evidence reads now use short PostgreSQL statement
deadlines inside read transactions, so a noncooperative database query cannot
hold both worker pool connections after the JavaScript stage deadline fires.

The source-read timeout, cooperative and noncooperative abort, claimed-job
exception, delayed retry timestamp, concurrent worker, stale recovery, and
published-sibling paths pass local integration tests with fake providers. The
full database suite passed 584 tests across 47 files on a temporary local
Postgres 18 database. After reinstalling the exact lockfile dependencies, all
1,353 unit tests passed across 151 files. Lint, Prisma validation/generation,
the Next.js build, and the AWS worker package build passed. The earlier local
unit failures came from stale `node_modules` versions, not the checked-in
lockfile. PR #169 is open from `a/activation-timeout-latency` for review.
A production worker deployment,
small distinct staging canary, queue-lag observation, and provider-backed
exercise audit remain unverified. Automatic retries still depend on the
30-to-120-second item backoff and the five-minute maintenance cadence when no
other continuation arrives. This branch does not promise a shorter live retry
than those bounds.
