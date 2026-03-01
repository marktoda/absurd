/**
 * Annotated Walkthrough — What Happens Under the Hood
 * ====================================================
 *
 * This example walks through an "invoice processing" workflow step by step.
 * Every SDK call is annotated with what actually happens: the SQL queries that
 * fire, the Postgres rows that get created, the locking strategies at play,
 * and the exact control flow when things go wrong.
 *
 * Run it with:
 *
 *   node --experimental-transform-types examples/annotated-walkthrough.ts
 *
 * Prerequisites:
 *   - Postgres running locally (or set ABSURD_DATABASE_URL)
 *   - Schema applied:  ./absurdctl init
 */
import { Absurd, TaskContext, TimeoutError } from "../src/index.ts";

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 1: CONSTRUCTION — Setting up the client                        ║
// ╚══════════════════════════════════════════════════════════════════════════╝

const app = new Absurd({
  queueName: "invoices",
});

// UNDER THE HOOD:
//
//   1. No connection string passed, so the SDK reads process.env.ABSURD_DATABASE_URL.
//      If that's also unset, it defaults to "postgresql://localhost/absurd".
//
//   2. A new pg.Pool is created with that connection string. The pool is lazy —
//      no TCP connections to Postgres exist yet. They'll be created on first query.
//      Because the SDK created this pool, it sets `ownedPool = true` so it knows
//      to close it later.
//
//   3. An in-memory Map<string, RegisteredTask> called `this.registry` is created.
//      It starts empty — no database calls happen at construction time.
//
//   4. `this.queueName` is set to "invoices". This is the default queue name used
//      for all operations unless overridden per-call.

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 2: QUEUE CREATION                                              ║
// ╚══════════════════════════════════════════════════════════════════════════╝

await app.createQueue();

// UNDER THE HOOD:
//
//   Executes: SELECT absurd.create_queue('invoices')
//
//   Inside Postgres, this stored procedure:
//
//   1. Validates the name (≤48 chars, alphanumeric + hyphens/underscores).
//
//   2. Inserts into the global registry:
//        INSERT INTO absurd.queues (queue_name, created_at)
//        VALUES ('invoices', clock_timestamp())
//        ON CONFLICT DO NOTHING;
//
//   3. Calls absurd.ensure_queue_tables('invoices') which creates FIVE tables:
//
//      ┌──────────────────────────────────────────────────────────────────┐
//      │  t_invoices  — Tasks (the logical unit of work)                 │
//      │  r_invoices  — Runs (each attempt to execute a task)            │
//      │  c_invoices  — Checkpoints (saved step return values)           │
//      │  e_invoices  — Events (emitted signals with payloads)           │
//      │  w_invoices  — Wait registrations (who's waiting for what)      │
//      └──────────────────────────────────────────────────────────────────┘
//
//      Each table is created with CREATE TABLE IF NOT EXISTS, so this is
//      idempotent. Indices are created on r_invoices for efficient claiming:
//        - (state, available_at) for the claim query
//        - (task_id) for run lookups
//      And on w_invoices:
//        - (event_name) for fast lookup during emit_event
//
//   This is the first actual TCP connection to Postgres — pg.Pool opens one
//   from its internal pool and keeps it alive for reuse.

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 3: TASK REGISTRATION — Purely in-memory                        ║
// ╚══════════════════════════════════════════════════════════════════════════╝

type InvoiceParams = {
  invoiceId: string;
  customerId: string;
  amount: number;
  currency: string;
};

app.registerTask<InvoiceParams>(
  {
    name: "process-invoice",
    defaultMaxAttempts: 4,
    defaultCancellation: { maxDuration: 300 }, // cancel if running > 5 min
  },
  async (params, ctx) => {
    // ─── This entire function body is the "task handler". ───
    // It doesn't run now. It's stored in the in-memory registry and
    // will be invoked later when a worker claims a task named
    // "process-invoice".

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  STEP 1: Validate the invoice                                    ║
    // ╚════════════════════════════════════════════════════════════════════╝

    const validation = await ctx.step("validate-invoice", async () => {
      // UNDER THE HOOD — FIRST EXECUTION:
      //
      //   1. ctx.step() calls getCheckpointName("validate-invoice").
      //      A counter tracks how many times each name is used. First call
      //      returns "validate-invoice" as-is. If you called step("validate-invoice")
      //      again, it would return "validate-invoice#2" to avoid collisions.
      //
      //   2. ctx.step() calls lookupCheckpoint("validate-invoice"):
      //        a) Checks the in-memory cache (Map<string, JsonValue>). Empty on first run.
      //        b) Queries Postgres:
      //             SELECT checkpoint_name, state, status, owner_run_id, updated_at
      //               FROM absurd.get_task_checkpoint_state('invoices', $taskId, 'validate-invoice')
      //        c) No row found → returns undefined.
      //
      //   3. Since no checkpoint exists, the lambda EXECUTES. This is the only
      //      time this code runs for this task, ever (unless the task is retried
      //      AND this specific step's checkpoint was lost — which doesn't happen
      //      because checkpoints survive across retries).
      //
      //   4. After the lambda returns, ctx.step() calls persistCheckpoint():
      //        SELECT absurd.set_task_checkpoint_state(
      //          'invoices',           -- queue
      //          $taskId,              -- task_id
      //          'validate-invoice',   -- step name
      //          '{"valid":true,...}', -- JSON-serialized return value
      //          $runId,               -- which run wrote this
      //          120                   -- extend claim by 120 seconds
      //        )
      //
      //      Inside Postgres, this procedure:
      //        a) Extends the worker's lease:
      //             UPDATE r_invoices
      //                SET claim_expires_at = now() + interval '120 seconds'
      //              WHERE run_id = $runId AND state = 'running'
      //
      //        b) Upserts the checkpoint (newer attempts always win):
      //             INSERT INTO c_invoices (task_id, checkpoint_name, state, ...)
      //             VALUES ($taskId, 'validate-invoice', $jsonState, ...)
      //             ON CONFLICT (task_id, checkpoint_name)
      //             DO UPDATE SET state = EXCLUDED.state, ...
      //               WHERE c_invoices.owner_run_id attempt <= new attempt
      //
      //        c) Checks if the task has been cancelled (raises AB001 if so).
      //
      //   5. The return value is cached in-memory: checkpointCache.set("validate-invoice", {...})
      //
      // UNDER THE HOOD — ON RETRY (if the task failed later and is re-executed):
      //
      //   When the worker claims the retried task, TaskContext.create() is called.
      //   This pre-loads ALL checkpoints for the task:
      //
      //     SELECT checkpoint_name, state, status, owner_run_id, updated_at
      //       FROM absurd.get_task_checkpoint_states('invoices', $taskId, $runId)
      //
      //   The results populate checkpointCache. So when ctx.step("validate-invoice")
      //   is called again, lookupCheckpoint() finds the value in the cache and
      //   returns it IMMEDIATELY — the lambda never runs a second time.
      //
      //   This is how Absurd achieves "exactly-once" semantics for steps.

      console.log(`Validating invoice ${params.invoiceId}`);
      if (params.amount <= 0) {
        throw new Error("Invalid invoice amount");
      }
      return { valid: true, validatedAt: new Date().toISOString() };
    });

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  STEP 2: Charge the payment                                      ║
    // ╚════════════════════════════════════════════════════════════════════╝

    const payment = await ctx.step("charge-payment", async () => {
      // Same checkpoint mechanics as step 1. On first run: execute + persist.
      // On retry: return cached result.
      //
      // IDEMPOTENCY TIP:
      //   ctx.taskID is a stable UUIDv7. You can derive idempotency keys from it:
      //     const idempotencyKey = `${ctx.taskID}:charge`;
      //   This way, even if the step body runs twice (e.g., the process crashed
      //   after the Stripe call but before the checkpoint was saved), the external
      //   system won't double-charge.

      console.log(`Charging ${params.amount} ${params.currency}`);
      return {
        chargeId: `ch_${params.invoiceId}`,
        amount: params.amount,
        currency: params.currency,
        chargedAt: new Date().toISOString(),
      };
    });

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  STEP 3: Sleep — wait for payment settlement                     ║
    // ╚════════════════════════════════════════════════════════════════════╝

    await ctx.sleepFor("wait-for-settlement", 10);

    // UNDER THE HOOD — FIRST CALL:
    //
    //   1. sleepFor("wait-for-settlement", 10) converts the duration to an
    //      absolute wake time: new Date(Date.now() + 10 * 1000).
    //      Then calls sleepUntil().
    //
    //   2. sleepUntil() checks for an existing checkpoint for "wait-for-settlement".
    //      None found on first execution.
    //
    //   3. Persists a checkpoint with the wake time as an ISO string:
    //        set_task_checkpoint_state('invoices', $taskId, 'wait-for-settlement',
    //          '"2025-06-15T10:30:10.000Z"', $runId, 120)
    //      This records WHEN we intend to wake up, not just that we slept.
    //
    //   4. Checks: is Date.now() < wakeAt? Yes (we just set it 10s in the future).
    //
    //   5. Calls scheduleRun():
    //        SELECT absurd.schedule_run('invoices', $runId, '2025-06-15T10:30:10.000Z')
    //
    //      Inside Postgres:
    //        a) Locks the run FOR UPDATE
    //        b) UPDATE r_invoices
    //              SET state = 'sleeping',
    //                  claimed_by = NULL,          -- releases the lease!
    //                  claim_expires_at = NULL,
    //                  available_at = $wakeAt       -- when to become claimable again
    //            WHERE run_id = $runId
    //        c) UPDATE t_invoices SET state = 'sleeping' WHERE task_id = $taskId
    //
    //   6. Throws SuspendTask (a special exception class).
    //
    //   7. Back in the worker's executeTask(), the catch block sees SuspendTask
    //      and returns silently — the run is NOT marked as failed.
    //
    //   The worker is now free to claim other tasks. This run sits in Postgres
    //   with state='sleeping' until available_at is reached.
    //
    // UNDER THE HOOD — WHEN THE SLEEP EXPIRES:
    //
    //   Some worker (possibly a different one!) calls absurd.claim_task().
    //   The claim query finds this run because:
    //     r.state = 'sleeping' AND r.available_at <= now()
    //
    //   The run is claimed, and the task handler executes FROM THE TOP. But:
    //   - Steps 1-2 ("validate-invoice", "charge-payment") hit cached checkpoints
    //     and return instantly without re-executing.
    //   - sleepUntil("wait-for-settlement") finds its checkpoint, sees the wake
    //     time has passed (Date.now() >= wakeAt), and DOES NOT throw SuspendTask.
    //     Execution continues to step 4.

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  STEP 4: Await an external event                                 ║
    // ╚════════════════════════════════════════════════════════════════════╝

    let receipt: unknown;
    try {
      receipt = await ctx.awaitEvent(
        `invoice.settled:${params.invoiceId}`,
        { timeout: 60 },
      );
    } catch (err) {
      if (err instanceof TimeoutError) {
        // Event didn't arrive within 60 seconds
        return { status: "settlement-timeout", invoiceId: params.invoiceId };
      }
      throw err;
    }

    // UNDER THE HOOD — awaitEvent():
    //
    //   The event system is the most sophisticated part of Absurd. Here's
    //   the full protocol:
    //
    //   1. Generates a step name: "$awaitEvent:invoice.settled:INV-001"
    //
    //   2. Checks the in-memory checkpoint cache. On retry, if the event
    //      was already received, returns the cached payload immediately.
    //
    //   3. Checks for timeout condition: if this run was previously sleeping
    //      for this event and woke up without a payload, it means the
    //      timeout expired. Throws TimeoutError.
    //
    //   4. Calls the stored procedure:
    //        SELECT should_suspend, payload
    //          FROM absurd.await_event(
    //            'invoices',                          -- queue
    //            $taskId,                             -- task_id
    //            $runId,                              -- run_id
    //            '$awaitEvent:invoice.settled:INV-001', -- checkpoint name
    //            'invoice.settled:INV-001',            -- event name
    //            60                                    -- timeout seconds
    //          )
    //
    //   Inside Postgres, this is a carefully choreographed dance:
    //
    //   ┌─────────────────────────────────────────────────────────────────┐
    //   │  a) Check if checkpoint already exists for this step.          │
    //   │     If yes → return (should_suspend=false, payload). Done.     │
    //   │                                                                │
    //   │  b) Ensure an event row exists (reservation):                  │
    //   │       INSERT INTO e_invoices (event_name, payload, emitted_at) │
    //   │       VALUES ('invoice.settled:INV-001', NULL, 'epoch')        │
    //   │       ON CONFLICT DO NOTHING;                                  │
    //   │     (payload=NULL is the sentinel for "not yet emitted")       │
    //   │                                                                │
    //   │  c) Lock the event row FOR SHARE:                              │
    //   │       SELECT 1 FROM e_invoices                                 │
    //   │        WHERE event_name = 'invoice.settled:INV-001'            │
    //   │        FOR SHARE;                                              │
    //   │     This coordinates with emit_event() without deadlocking.    │
    //   │     Lock ordering: event first (SHARE), then run (UPDATE).     │
    //   │                                                                │
    //   │  d) Lock the run FOR UPDATE:                                   │
    //   │       SELECT r.state, r.event_payload, ...                     │
    //   │         FROM r_invoices r JOIN t_invoices t ...                 │
    //   │        WHERE r.run_id = $runId FOR UPDATE;                     │
    //   │                                                                │
    //   │  e) Check if event has been emitted (payload IS NOT NULL):     │
    //   │     • YES → Write checkpoint, return (false, payload). Done.   │
    //   │     • NO  → Continue to suspend.                               │
    //   │                                                                │
    //   │  f) Register a wait:                                           │
    //   │       INSERT INTO w_invoices                                   │
    //   │         (task_id, run_id, step_name, event_name, timeout_at)   │
    //   │       VALUES ($taskId, $runId, $stepName, $eventName,          │
    //   │               now() + interval '60 seconds');                  │
    //   │                                                                │
    //   │  g) Suspend the run:                                           │
    //   │       UPDATE r_invoices SET                                    │
    //   │         state = 'sleeping',                                    │
    //   │         claimed_by = NULL,                                     │
    //   │         claim_expires_at = NULL,                               │
    //   │         available_at = now() + 60s,  -- timeout deadline       │
    //   │         wake_event = 'invoice.settled:INV-001'                 │
    //   │       WHERE run_id = $runId;                                   │
    //   │       UPDATE t_invoices SET state = 'sleeping' ...;            │
    //   │                                                                │
    //   │  h) Return (should_suspend=true, null).                        │
    //   └─────────────────────────────────────────────────────────────────┘
    //
    //   5. Back in the SDK: should_suspend is true, so throw SuspendTask().
    //      The worker releases this task, same as with sleep.
    //
    // RACE-FREE GUARANTEE:
    //
    //   What if emit_event() fires at the exact same moment?
    //
    //   emit_event() does an UPSERT on the event row (acquiring a row lock),
    //   then finds all waiters. Because await_event() holds a FOR SHARE lock
    //   on the event row and checks the payload AFTER locking, the two
    //   operations are serialized:
    //
    //   • If emit happens first: await_event sees payload, returns immediately.
    //   • If await happens first: registers wait, emit finds and wakes it.
    //   • They can't both miss each other.

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  STEP 5: Final step — send confirmation                         ║
    // ╚════════════════════════════════════════════════════════════════════╝

    const confirmation = await ctx.step("send-confirmation", async () => {
      console.log(`Invoice ${params.invoiceId} settled, sending confirmation`);
      return {
        invoiceId: params.invoiceId,
        chargeId: payment.chargeId,
        receipt,
        confirmedAt: new Date().toISOString(),
      };
    });

    // ╔════════════════════════════════════════════════════════════════════╗
    // ║  TASK COMPLETION                                                 ║
    // ╚════════════════════════════════════════════════════════════════════╝

    return confirmation;

    // UNDER THE HOOD:
    //
    //   When the handler returns without throwing, executeTask() calls:
    //
    //     SELECT absurd.complete_run('invoices', $runId, $resultJson)
    //
    //   Inside Postgres:
    //     1. Locks the run FOR UPDATE (prevents concurrent fail/complete race)
    //     2. Validates the run is in 'running' state
    //     3. UPDATE r_invoices SET state='completed', result=$result, completed_at=now()
    //     4. UPDATE t_invoices SET state='completed', completed_payload=$result
    //     5. DELETE FROM w_invoices WHERE run_id = $runId  (cleanup)
    //
    //   The task is done. Its checkpoints remain in c_invoices until cleaned
    //   up by absurd.cleanup_tasks().
  },
);

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 4: SPAWNING A TASK                                             ║
// ╚══════════════════════════════════════════════════════════════════════════╝

const { taskID, runID, created } = await app.spawn("process-invoice", {
  invoiceId: "INV-001",
  customerId: "CUST-42",
  amount: 9999,
  currency: "USD",
}, {
  retryStrategy: { kind: "exponential", baseSeconds: 5, factor: 2, maxSeconds: 60 },
  idempotencyKey: "invoice:INV-001",
});

console.log(`Spawned task=${taskID} run=${runID} created=${created}`);

// UNDER THE HOOD:
//
//   1. The SDK resolves the task registration and merges options:
//      - maxAttempts: 4 (from defaultMaxAttempts on the registration)
//      - cancellation: { max_duration: 300 } (from defaultCancellation)
//      - retryStrategy and idempotencyKey from the spawn call
//
//   2. If a beforeSpawn hook were registered, it would fire here to
//      modify options (e.g., inject trace headers).
//
//   3. Options are normalized from camelCase to snake_case for Postgres:
//      { retry_strategy: {...}, max_attempts: 4, cancellation: {...},
//        idempotency_key: "invoice:INV-001" }
//
//   4. Executes:
//        SELECT task_id, run_id, attempt, created
//          FROM absurd.spawn_task('invoices', 'process-invoice', $paramsJson, $optionsJson)
//
//   Inside Postgres:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │  a) Generate UUIDv7 for task_id:                                   │
//   │     absurd.portable_uuidv7() → timestamp-sortable UUID            │
//   │     (Uses native uuidv7() on PG18+, manual construction otherwise) │
//   │                                                                    │
//   │  b) INSERT INTO t_invoices:                                        │
//   │     (task_id, task_name, params, headers, retry_strategy,          │
//   │      max_attempts, cancellation, enqueue_at, state, attempts,      │
//   │      idempotency_key)                                              │
//   │     VALUES ($uuid, 'process-invoice', $params, '{}',              │
//   │      '{"kind":"exponential","base_seconds":5,"factor":2,...}',     │
//   │      4, '{"max_duration":300}', now(), 'pending', 1,              │
//   │      'invoice:INV-001')                                            │
//   │                                                                    │
//   │     ON CONFLICT (idempotency_key) DO NOTHING                       │
//   │     ─── If the key already exists, no insert happens.              │
//   │                                                                    │
//   │  c) If the insert was a no-op (idempotency hit):                   │
//   │     Look up the existing task and return its IDs with created=false │
//   │                                                                    │
//   │  d) If the insert succeeded:                                       │
//   │     Generate another UUIDv7 for the first run.                     │
//   │     INSERT INTO r_invoices:                                        │
//   │       (run_id, task_id, attempt, state, available_at)              │
//   │       VALUES ($runUuid, $taskUuid, 1, 'pending', now())            │
//   │     Return with created=true.                                      │
//   └─────────────────────────────────────────────────────────────────────┘
//
//   At this point, the database contains:
//
//   t_invoices: 1 row  — state='pending', attempts=1
//   r_invoices: 1 row  — state='pending', available_at=now
//   c_invoices: 0 rows — no checkpoints yet
//   e_invoices: 0 rows — no events yet
//   w_invoices: 0 rows — no waits yet

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 5: STARTING THE WORKER                                         ║
// ╚══════════════════════════════════════════════════════════════════════════╝

const worker = await app.startWorker({
  concurrency: 2,
  claimTimeout: 120,
  pollInterval: 0.5,
});

// UNDER THE HOOD:
//
//   startWorker() launches an async polling loop. Here's the lifecycle:
//
//   ┌─────────────────────── Worker Loop ────────────────────────────────┐
//   │                                                                    │
//   │  while (running) {                                                 │
//   │    // 1. Concurrency gate                                          │
//   │    if (executing.size >= concurrency) {                            │
//   │      await waitForAvailability();  // block until a slot opens     │
//   │      continue;                                                     │
//   │    }                                                               │
//   │                                                                    │
//   │    // 2. Claim tasks from Postgres                                 │
//   │    const tasks = await claimTasks({                                │
//   │      batchSize: min(batchSize, availableCapacity),                 │
//   │      claimTimeout: 120,                                            │
//   │      workerId: "hostname:pid"                                      │
//   │    });                                                             │
//   │                                                                    │
//   │    // 3. If nothing claimed, sleep pollInterval then retry         │
//   │    if (tasks.length === 0) {                                       │
//   │      await waitForAvailability(); // sleeps 0.5s via setTimeout    │
//   │      continue;                                                     │
//   │    }                                                               │
//   │                                                                    │
//   │    // 4. Fire-and-forget each claimed task                         │
//   │    for (const task of tasks) {                                     │
//   │      const p = executeTask(task, 120)                              │
//   │        .catch(onError)                                             │
//   │        .finally(() => {                                            │
//   │          executing.delete(p);                                      │
//   │          notifyAvailability(); // wake loop immediately            │
//   │        });                                                         │
//   │      executing.add(p);                                             │
//   │    }                                                               │
//   │  }                                                                 │
//   │  await Promise.allSettled(executing); // graceful shutdown         │
//   │                                                                    │
//   └────────────────────────────────────────────────────────────────────┘
//
//   The claimTasks() call executes:
//     SELECT * FROM absurd.claim_task('invoices', 'hostname:pid', 120, 2)
//
//   Inside Postgres, claim_task runs THREE PHASES atomically:
//
//   ── Phase 1: Cancellation enforcement ──
//   Scans t_invoices for tasks violating their cancellation policy:
//     - max_delay: task pending too long before first_started_at
//     - max_duration: task running too long since first_started_at
//   Matching tasks are set to state='cancelled'.
//
//   ── Phase 2: Expired lease recovery ──
//   Finds runs where claim_expires_at ≤ now() (worker crashed or too slow):
//     SELECT run_id FROM r_invoices
//      WHERE state = 'running'
//        AND claim_expires_at <= now()
//      FOR UPDATE SKIP LOCKED
//   For each expired run, calls absurd.fail_run() with a $ClaimTimeout
//   error, which schedules a retry with the configured backoff.
//   SKIP LOCKED prevents thundering herd when multiple workers claim
//   simultaneously.
//
//   ── Phase 3: Claim available runs ──
//     WITH candidate AS (
//       SELECT r.run_id FROM r_invoices r
//         JOIN t_invoices t ON t.task_id = r.task_id
//        WHERE r.state IN ('pending', 'sleeping')
//          AND t.state IN ('pending', 'sleeping', 'running')
//          AND r.available_at <= now()
//        ORDER BY r.available_at, r.run_id  -- FIFO ordering
//        LIMIT 2                            -- batchSize
//        FOR UPDATE SKIP LOCKED             -- non-blocking concurrency
//     )
//     UPDATE r_invoices SET
//       state = 'running',
//       claimed_by = 'hostname:pid',
//       claim_expires_at = now() + interval '120 seconds',
//       started_at = now()
//     WHERE run_id IN (SELECT run_id FROM candidate)
//
//   Also updates t_invoices: state='running', first_started_at=now()
//
//   Returns the claimed tasks with their params, headers, retry config,
//   and any cached event_payload (for event-driven resumes).

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 6: TASK EXECUTION — What happens inside executeTask()          ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// The worker claimed our task. Here's what executeTask() does:
//
//   1. Creates a TaskContext:
//        - Loads ALL existing checkpoints for this task into memory:
//            SELECT * FROM absurd.get_task_checkpoint_states('invoices', $taskId, $runId)
//          On first run, this returns 0 rows.
//          On retries, returns all previously saved checkpoints.
//
//   2. Sets up lease timeout safety nets:
//        - Warning timer at claimTimeout (120s): logs a warning
//        - Fatal timer at 2×claimTimeout (240s): process.exit(1)
//          (This prevents zombie workers from running forever after
//           their lease has expired and another worker has taken over.)
//
//   3. If a wrapTaskExecution hook is configured, wraps the handler:
//        await hooks.wrapTaskExecution(ctx, async () => {
//          const result = await handler(params, ctx);
//          await completeRun(..., result);
//        });
//      Otherwise, calls the handler directly.
//
//   4. Exception handling:
//        - SuspendTask  → return silently (task suspended for sleep/event)
//        - CancelledTask → return silently (task was cancelled mid-execution)
//        - Any other error → call failTaskRun():
//
//            SELECT absurd.fail_run('invoices', $runId, $errorJson, NULL)
//
//          Inside Postgres, fail_run():
//            a) Marks the run as 'failed' with the error details
//            b) Calculates the retry delay based on retry_strategy:
//               • exponential: 5 × 2^(attempt-1) → 5s, 10s, 20s, 40s (capped at 60s)
//               • fixed: constant delay
//               • none: immediate retry (0s delay)
//            c) Checks cancellation policy (would the retry exceed max_duration?)
//            d) If retries remain and not cancelled:
//               Creates a NEW run in r_invoices with:
//                 state = 'sleeping' (if delayed) or 'pending' (if immediate)
//                 available_at = now() + delay
//            e) If max_attempts exceeded or cancelled:
//               Sets task state to 'failed' or 'cancelled'. No new run created.

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 7: EMITTING AN EVENT FROM OUTSIDE                              ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// Simulate an external system settling the invoice after a delay.
// In a real app, this would be a webhook handler or another service.
setTimeout(async () => {
  await app.emitEvent(`invoice.settled:INV-001`, {
    settledAt: new Date().toISOString(),
    processorRef: "PROC-789",
  }, "invoices");

  // UNDER THE HOOD:
  //
  //   Executes:
  //     SELECT absurd.emit_event('invoices', 'invoice.settled:INV-001', $payloadJson)
  //
  //   Inside Postgres, this is an atomic multi-table operation:
  //
  //   ┌─────────────────────────────────────────────────────────────────┐
  //   │  1. UPSERT the event row (sets payload, which was NULL):       │
  //   │       INSERT INTO e_invoices (event_name, payload, emitted_at) │
  //   │       VALUES ('invoice.settled:INV-001', $payload, now())      │
  //   │       ON CONFLICT (event_name)                                 │
  //   │       DO UPDATE SET payload = $payload, emitted_at = now();    │
  //   │                                                                │
  //   │  2. Find all sleeping waiters for this event:                  │
  //   │       SELECT run_id, task_id, step_name                        │
  //   │         FROM w_invoices                                        │
  //   │        WHERE event_name = 'invoice.settled:INV-001'            │
  //   │          AND (timeout_at IS NULL OR timeout_at > now())        │
  //   │                                                                │
  //   │  3. Wake each waiter:                                          │
  //   │       UPDATE r_invoices SET                                    │
  //   │         state = 'pending',                                     │
  //   │         available_at = now(),   -- immediately claimable!      │
  //   │         wake_event = NULL,                                     │
  //   │         event_payload = $payload  -- cached for SDK recovery   │
  //   │       WHERE run_id IN (affected) AND state = 'sleeping'        │
  //   │                                                                │
  //   │  4. Write checkpoints for each awakened step:                  │
  //   │       INSERT INTO c_invoices                                   │
  //   │         (task_id, checkpoint_name, state, ...)                  │
  //   │       VALUES ($taskId, '$awaitEvent:invoice.settled:INV-001',  │
  //   │               $payload, ...)                                    │
  //   │       ON CONFLICT DO UPDATE ...                                │
  //   │                                                                │
  //   │  5. Update task states:                                        │
  //   │       UPDATE t_invoices SET state = 'pending'                  │
  //   │        WHERE task_id IN (awakened tasks)                       │
  //   │                                                                │
  //   │  6. Clean up wait registrations:                               │
  //   │       DELETE FROM w_invoices                                   │
  //   │        WHERE event_name = 'invoice.settled:INV-001'            │
  //   │          AND run_id IN (awakened runs)                         │
  //   └─────────────────────────────────────────────────────────────────┘
  //
  //   All of this happens in ONE SQL statement (a single CTE chain),
  //   so it's fully atomic. Either all waiters wake up or none do.
  //
  //   The next time any worker calls claim_task(), it will find this
  //   run with state='pending' and available_at=now(), claim it, and
  //   re-execute the task handler from the top — but all prior steps
  //   will hit cached checkpoints and the awaitEvent() call will find
  //   its checkpoint with the event payload and return immediately.
}, 15_000);

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  PHASE 8: SHUTDOWN                                                    ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// Let the example run for 30 seconds, then shut down.
setTimeout(async () => {
  console.log("Shutting down...");

  await worker.close();
  // UNDER THE HOOD:
  //   Sets `running = false` in the worker loop.
  //   The loop exits its while() and calls:
  //     await Promise.allSettled(executing)
  //   This waits for all in-flight tasks to finish (they won't be
  //   interrupted — Absurd doesn't kill running tasks).

  await app.close();
  // UNDER THE HOOD:
  //   Because ownedPool = true, calls pool.end() which:
  //     1. Waits for all checked-out clients to be returned
  //     2. Closes all TCP connections to Postgres
  //     3. Resolves the promise

  console.log("Done.");
}, 30_000);

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APPENDIX A: Complete Timeline of a Successful Task                   ║
// ╚══════════════════════════════════════════════════════════════════════════╝
//
//   t=0s   spawn_task()        → t_invoices: pending,  r_invoices: pending
//   t=0.5s claim_task()        → t_invoices: running,  r_invoices: running (claimed by worker)
//   t=0.5s step("validate")    → c_invoices: 1 row     (claim extended to t=120.5s)
//   t=0.5s step("charge")      → c_invoices: 2 rows    (claim extended to t=120.5s)
//   t=0.5s sleepFor(10s)       → c_invoices: 3 rows, task sleeps until t=10.5s
//                                 r_invoices: sleeping, available_at=t+10.5s
//                                 worker releases the task
//   t=10.5s claim_task()       → r_invoices: running again (same or different worker)
//                                 steps 1-3 replay from cache instantly
//   t=10.5s awaitEvent()       → task suspends, waiting for event
//                                 w_invoices: 1 row (timeout at t=70.5s)
//                                 r_invoices: sleeping, wake_event set
//   t=15s  emit_event()        → event written, run woken to 'pending'
//                                 c_invoices: 4 rows (event payload checkpointed)
//                                 w_invoices: cleaned up
//   t=15.5s claim_task()       → r_invoices: running again
//                                 steps 1-4 replay from cache instantly
//   t=15.5s step("send-conf")  → c_invoices: 5 rows
//   t=15.5s complete_run()     → t_invoices: completed, r_invoices: completed
//
// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APPENDIX B: What Happens When Things Go Wrong                        ║
// ╚══════════════════════════════════════════════════════════════════════════╝
//
//   SCENARIO: Worker crashes after step 2 but before sleepFor checkpoint
//   ──────────────────────────────────────────────────────────────────────
//   - The run's claim_expires_at is still set (was extended at step 2).
//   - After 120s, another worker's claim_task() detects the expired lease.
//   - Phase 2 of claim_task fails the run with $ClaimTimeout error.
//   - A new run is created with exponential backoff (5s for attempt 2).
//   - New worker claims and re-executes. Steps 1 and 2 return cached
//     results. sleepFor runs fresh (no checkpoint saved before crash).
//
//   SCENARIO: step("charge-payment") throws an error
//   ──────────────────────────────────────────────────
//   - executeTask catches the error and calls fail_run().
//   - fail_run calculates: 5 × 2^(1-1) = 5 second delay.
//   - New run created: state='sleeping', available_at=now()+5s.
//   - After 5s, worker claims the retry. step("validate-invoice")
//     returns cached result. step("charge-payment") runs again fresh.
//
//   SCENARIO: Task exceeds max_duration (300s)
//   ───────────────────────────────────────────
//   - At claim_task() time, Phase 1 checks:
//       extract(epoch from (now() - first_started_at)) >= 300
//   - If true, task is set to 'cancelled'.
//   - Also checked at retry time in fail_run(): if the next retry's
//     available_at would exceed max_duration, cancel instead of retry.
//   - Once cancelled, any checkpoint write or heartbeat raises AB001
//     (CancelledTask), which the SDK catches and returns silently.
//
//   SCENARIO: Two workers claim the same task (overlapping execution)
//   ─────────────────────────────────────────────────────────────────
//   - This CAN happen if Worker A's claim expires while it's still
//     running (slow step), and Worker B claims the retry.
//   - Both workers execute the handler. But:
//     a) Steps use checkpoints with attempt-based ordering — the newer
//        attempt's checkpoints take precedence.
//     b) complete_run() validates the run is still in 'running' state.
//        The losing worker's call will fail (run already completed/failed).
//     c) The fatalOnLeaseTimeout safety (2× timeout) will kill Worker A.
//   - For external side effects, use ctx.taskID as an idempotency key.
