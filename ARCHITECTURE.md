# Absurd Architecture Document

This document provides a comprehensive technical overview of the Absurd codebase — a
Postgres-native durable workflow system that moves execution complexity into database
stored procedures, keeping client SDKs lightweight and language-agnostic.

## Table of Contents

- [Design Philosophy](#design-philosophy)
- [Repository Layout](#repository-layout)
- [Core SQL Layer](#core-sql-layer)
  - [Schema Overview](#schema-overview)
  - [Per-Queue Tables](#per-queue-tables)
  - [Utility Functions](#utility-functions)
  - [Queue Management](#queue-management)
  - [Task Lifecycle](#task-lifecycle)
  - [Checkpoint System](#checkpoint-system)
  - [Event System](#event-system)
  - [Cancellation](#cancellation)
  - [Cleanup](#cleanup)
  - [Concurrency and Locking](#concurrency-and-locking)
- [TypeScript SDK](#typescript-sdk)
  - [Core API](#core-api)
  - [Task Registration and Execution](#task-registration-and-execution)
  - [Worker Model](#worker-model)
  - [Hooks System](#hooks-system)
- [Python SDK](#python-sdk)
- [Habitat Web UI](#habitat-web-ui)
  - [Go Backend](#go-backend)
  - [SolidJS Frontend](#solidjs-frontend)
  - [API Endpoints](#api-endpoints)
- [absurdctl CLI](#absurdctl-cli)
- [Test Suite](#test-suite)
- [Build and CI/CD](#build-and-cicd)
- [Release Process](#release-process)

---

## Design Philosophy

Absurd is built on a single key principle: **the database is the execution engine**. All
durable state, scheduling, retry logic, event coordination, and concurrency control live
inside Postgres stored procedures. SDKs are thin wrappers that call these procedures and
map results into language-idiomatic patterns.

This design means:

- **No coordinator service** — Postgres is the only infrastructure dependency.
- **Pull-based** — Workers pull tasks from the database; there is no push/webhook layer.
- **Language-agnostic** — Any language that can talk to Postgres can implement an SDK.
- **Checkpointed steps** — Tasks decompose into steps whose results are cached, giving
  "exactly-once" semantics for step bodies even across retries.
- **Race-free events** — Events are cached in the database; a task that waits for an
  already-emitted event resolves immediately.

---

## Repository Layout

```
absurd/
├── sql/
│   ├── absurd.sql              # Core schema and all stored procedures (~1,400 lines)
│   └── migrations/             # Versioned migration files (0.0.3→0.0.4, etc.)
├── sdks/
│   ├── typescript/             # TypeScript SDK (main, published as absurd-sdk)
│   │   ├── src/index.ts        # Single-file SDK implementation (~1,022 lines)
│   │   ├── test/               # Vitest tests (7 files)
│   │   └── examples/           # Sleep, provisioning, agent-loop examples
│   └── python/                 # Python SDK (unpublished)
│       ├── src/absurd_sdk/     # Sync + async client (~1,428 lines)
│       └── tests/              # pytest tests (8 files)
├── habitat/                    # Go-based web UI dashboard
│   ├── cmd/habitat/main.go     # Entry point
│   ├── internal/               # Server, config, handlers, embedded assets
│   └── ui/                     # SolidJS frontend (Vite + Tailwind)
├── absurdctl                   # Python CLI tool for queue/task management
├── tests/                      # Core SQL integration tests (Python + testcontainers)
├── scripts/
│   ├── release.sh              # Automated release script
│   └── validate-psql           # Schema migration validation
├── Makefile                    # Top-level build/test/format targets
└── CHANGELOG.md                # Version history (0.0.1 through 0.0.7)
```

---

## Core SQL Layer

**File:** `sql/absurd.sql`

Everything lives in the `absurd` schema. The `uuid-ossp` extension is loaded as a
fallback for UUID generation on older Postgres versions.

### Schema Overview

A global metadata table tracks queues:

```
absurd.queues
  └── queue_name text PRIMARY KEY
  └── created_at timestamptz
```

Each queue dynamically creates 5 tables (described below). All stored procedures accept
a `p_queue_name` parameter and use `format()` with `%I` identifier escaping to build
dynamic SQL against the correct tables.

### Per-Queue Tables

When `absurd.create_queue('myqueue')` is called, these tables are created:

#### `t_myqueue` — Tasks

The logical unit of work. A task is submitted, claimed by a worker, and eventually
completes, fails, or is cancelled.

| Column | Type | Description |
|--------|------|-------------|
| `task_id` | `uuid` (PK) | UUIDv7, sortable by creation time |
| `task_name` | `text` | Task type name (e.g., `"order-fulfillment"`) |
| `params` | `jsonb` | Input parameters |
| `headers` | `jsonb` | Custom metadata headers |
| `retry_strategy` | `jsonb` | `{kind, base_seconds, factor, max_seconds}` |
| `max_attempts` | `integer` | Max retries (NULL = unlimited) |
| `cancellation` | `jsonb` | `{max_delay, max_duration}` in seconds |
| `enqueue_at` | `timestamptz` | When the task was enqueued |
| `first_started_at` | `timestamptz` | When first claimed by a worker |
| `state` | `text` | `pending`, `running`, `sleeping`, `completed`, `failed`, `cancelled` |
| `attempts` | `integer` | Highest attempt number |
| `last_attempt_run` | `uuid` | Most recent run ID |
| `completed_payload` | `jsonb` | Final result (on completion) |
| `cancelled_at` | `timestamptz` | Cancellation timestamp |
| `idempotency_key` | `text` (unique) | Optional deduplication key |

#### `r_myqueue` — Runs

Each attempt to execute a task creates a run. A task may have many runs across retries.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | `uuid` (PK) | UUIDv7 |
| `task_id` | `uuid` (FK) | Parent task |
| `attempt` | `integer` | Attempt number (1, 2, 3, …) |
| `state` | `text` | Same states as tasks |
| `claimed_by` | `text` | Worker ID holding the lease |
| `claim_expires_at` | `timestamptz` | Lease expiration |
| `available_at` | `timestamptz` | Earliest time this run can be claimed |
| `wake_event` | `text` | Event name if sleeping for an event |
| `event_payload` | `jsonb` | Cached event data for idempotent replay |
| `started_at` | `timestamptz` | When claimed |
| `completed_at` | `timestamptz` | Completion time |
| `failed_at` | `timestamptz` | Failure time |
| `result` | `jsonb` | Final result |
| `failure_reason` | `jsonb` | Error details `{name, message, stack}` |

Indexed on `(state, available_at)` for efficient claiming and `(task_id)` for lookups.

#### `c_myqueue` — Checkpoints

Step results cached for durability. Primary key is `(task_id, checkpoint_name)`, so each
step name is stored once per task and newer attempts overwrite older ones.

| Column | Type | Description |
|--------|------|-------------|
| `task_id` | `uuid` | Parent task |
| `checkpoint_name` | `text` | Step name |
| `state` | `jsonb` | Cached return value |
| `status` | `text` | Always `'committed'` |
| `owner_run_id` | `uuid` | Which run wrote this |
| `updated_at` | `timestamptz` | Last write time |

#### `e_myqueue` — Events

One row per event name. `payload IS NULL` is the sentinel for "not yet emitted".

| Column | Type | Description |
|--------|------|-------------|
| `event_name` | `text` (PK) | Unique event identifier |
| `payload` | `jsonb` | Event data (NULL until emitted) |
| `emitted_at` | `timestamptz` | Emission time |

#### `w_myqueue` — Wait Registrations

Tracks which runs are waiting for which events and their timeout deadlines.

| Column | Type | Description |
|--------|------|-------------|
| `task_id` | `uuid` | Waiting task |
| `run_id` | `uuid` | Waiting run |
| `step_name` | `text` | Checkpoint name for this wait |
| `event_name` | `text` | Event being awaited |
| `timeout_at` | `timestamptz` | Deadline (NULL = wait forever) |

Primary key: `(run_id, step_name)`. Indexed on `event_name` for fast lookup during emission.

### Utility Functions

#### `absurd.current_time()` → `timestamptz`

Returns `clock_timestamp()` normally, or the value of the `absurd.fake_now` session
variable if set. This allows tests to control time deterministically.

#### `absurd.portable_uuidv7()` → `uuid`

Generates UUIDv7 (timestamp-sortable) IDs. On Postgres 18+, delegates to the native
`uuidv7()` function. On older versions, manually constructs the UUID from the current
timestamp in milliseconds plus random bytes, setting the version and variant bits.

### Queue Management

| Function | Description |
|----------|-------------|
| `create_queue(name)` | Validates name (≤48 chars), inserts into `absurd.queues`, calls `ensure_queue_tables()` to create the 5 tables. Silently no-ops on duplicates. |
| `drop_queue(name)` | Drops all 5 tables with `CASCADE`, removes from `absurd.queues`. |
| `list_queues()` | Returns all queue names sorted alphabetically. |

### Task Lifecycle

#### Spawning: `absurd.spawn_task(queue, task_name, params, options)`

Creates a task row and its first pending run. Returns `(task_id, run_id, attempt, created)`.

Options include: `headers`, `retry_strategy`, `max_attempts`, `cancellation`, and
`idempotency_key`. When an idempotency key collides, the existing task is returned with
`created = false`.

#### Claiming: `absurd.claim_task(queue, worker_id, claim_timeout, qty)`

Workers call this to pull work. Executes in three phases:

1. **Cancellation enforcement** — Scans for tasks exceeding their `max_delay` (pending
   too long) or `max_duration` (running too long) and marks them cancelled.
2. **Expired lease recovery** — Finds running runs where `claim_expires_at ≤ now`, calls
   `fail_run()` with a `$ClaimTimeout` error to schedule retries.
3. **Claim available runs** — Selects up to `qty` pending/sleeping runs ordered by
   `(available_at, run_id)` for FIFO fairness, sets them to `running`, assigns the
   worker, and sets the lease deadline.

#### Completing: `absurd.complete_run(queue, run_id, state)`

Marks a run and its parent task as `completed`. Validates the run is in `running` state.
Cleans up wait registrations.

#### Sleeping: `absurd.schedule_run(queue, run_id, wake_at)`

Suspends a run until `wake_at`. Sets the run to `sleeping` with `available_at = wake_at`,
clears the claim, and updates the task state to `sleeping`.

#### Failing: `absurd.fail_run(queue, run_id, reason, retry_at)`

Marks a run as failed and determines the next action:

1. If `max_attempts` not reached, calculates the retry delay:
   - **`fixed`**: constant `base_seconds` delay (default 60s)
   - **`exponential`**: `base × factor^(attempt-1)`, capped at `max_seconds`
   - **`none`**: immediate retry (0 delay)
2. Checks cancellation policy — if `max_duration` would be exceeded by the retry time,
   cancels instead of retrying.
3. Creates a new run in `pending` (if ready now) or `sleeping` (if delayed) state.
4. If max attempts exceeded, no new run is created and the task stays `failed`.

### Checkpoint System

#### `set_task_checkpoint_state(queue, task_id, step_name, state, owner_run, extend_claim_by)`

Saves a step result. If `extend_claim_by` is provided, extends the run's lease. Newer
run attempts overwrite checkpoints from older attempts. Raises `AB001` if the task has
been cancelled.

#### `get_task_checkpoint_states(queue, task_id, run_id)`

Loads all checkpoints for a task, ordered chronologically. Used on retry to pre-populate
the in-memory cache so completed steps are skipped.

### Event System

#### `await_event(queue, task_id, run_id, step_name, event_name, timeout)`

Race-safe event waiting with this logic:

1. If a checkpoint already exists for this step, return it immediately (already completed).
2. Insert an event row with `payload = NULL` if none exists (reservation).
3. Lock the event row `FOR SHARE` to coordinate with concurrent `emit_event` calls.
4. If the event has been emitted (payload not null), save a checkpoint and return
   `(should_suspend=false, payload)`.
5. Otherwise, register a wait in `w_` table, set the run to `sleeping`, and return
   `(should_suspend=true, null)`.

Returns `should_suspend` to tell the SDK whether the task should yield control.

#### `emit_event(queue, event_name, payload)`

Broadcasts an event. In a single atomic operation:

1. Upserts the event row with the payload.
2. Finds all sleeping runs waiting for this event.
3. Transitions them to `pending` with `available_at = now` (immediately claimable).
4. Writes checkpoints for each awakened step with the event payload.
5. Cleans up wait registrations.

### Cancellation

#### `cancel_task(queue, task_id)`

Manually cancels a task. Sets it to `cancelled`, marks all non-terminal runs as
`cancelled`, and cleans up wait registrations. No-ops if already in a terminal state.

#### `extend_claim(queue, run_id, extend_by)`

Extends a run's lease by N seconds. Used for long-running steps or heartbeats. Raises
`AB001` if the task is cancelled.

### Cleanup

| Function | Description |
|----------|-------------|
| `cleanup_tasks(queue, ttl_seconds, limit)` | Deletes terminal tasks (and their runs, checkpoints, waits) older than TTL. Returns count deleted. |
| `cleanup_events(queue, ttl_seconds, limit)` | Deletes emitted events older than TTL. Returns count deleted. |

Both accept a `limit` parameter (default 1000) for batch processing.

### Concurrency and Locking

The SQL layer uses careful locking to prevent races:

- **`FOR UPDATE`** on runs and tasks during claim, complete, fail, and cancel operations.
- **`FOR SHARE`** on event rows during `await_event` to coordinate with `emit_event`
  without deadlocking.
- **`INSERT ... ON CONFLICT DO NOTHING`** for idempotency key deduplication.
- **`INSERT ... ON CONFLICT DO UPDATE`** for checkpoint upserts (newer attempts overwrite).
- **Lease-based concurrency**: Workers hold a time-limited claim. If the claim expires
  without progress, the run is failed and retried by another worker.

---

## TypeScript SDK

**Location:** `sdks/typescript/` | **Package:** `absurd-sdk` (v0.0.7) | **File:** `src/index.ts` (~1,022 lines)

The TypeScript SDK is the primary SDK. It depends on `pg` (peer dependency) and
publishes both ESM and CommonJS builds.

### Core API

#### Constructor

```typescript
const app = new Absurd({
  db: pool | connectionString,    // pg.Pool or Postgres URL (default: env ABSURD_DATABASE_URL)
  queueName: 'default',           // Default queue name
  defaultMaxAttempts: 5,           // Default retry limit
  log: customLogger,               // Optional logger
  hooks: { beforeSpawn, wrapTaskExecution },
});
```

#### Key Methods

| Method | Description |
|--------|-------------|
| `registerTask(options, handler)` | Register a named task handler |
| `spawn(taskName, params, options?)` | Enqueue a task for execution |
| `emitEvent(eventName, payload?)` | Emit an event to wake waiting tasks |
| `cancelTask(taskID)` | Cancel a task |
| `startWorker(options?)` | Start a polling worker |
| `workBatch(workerId?, timeout?, batchSize?)` | Process a single batch of tasks |
| `claimTasks(options?)` | Manually claim tasks |
| `createQueue() / dropQueue() / listQueues()` | Queue management |
| `bindToConnection(con, owned?)` | Create client bound to a specific connection |
| `close()` | Shut down worker and close owned pool |

### Task Registration and Execution

```typescript
app.registerTask<Params, Result>(
  { name: 'order-fulfillment', defaultMaxAttempts: 3 },
  async (params, ctx) => {
    // ctx.step() — checkpointed step (cached on retry)
    const payment = await ctx.step('charge', async () => {
      return await stripe.charges.create({ amount: params.amount });
    });

    // ctx.sleepFor() — suspend for a duration
    await ctx.sleepFor('cooldown', 60);

    // ctx.awaitEvent() — suspend until event arrives
    const shipment = await ctx.awaitEvent('shipment.packed', { timeout: 3600 });

    // ctx.emitEvent() — emit an event
    await ctx.emitEvent('order.completed', { orderId: payment.id });

    // ctx.heartbeat() — extend lease
    await ctx.heartbeat(30);

    return { orderId: payment.id };
  }
);
```

**TaskContext properties:**
- `taskID` — unique task identifier (useful for deriving idempotency keys)
- `headers` — metadata headers from spawn

**Checkpoint behavior:**
- On first execution, `step()` runs the function and saves the result.
- On retry, all previously completed checkpoints are loaded into memory before execution
  begins. When `step()` is called, the cached result is returned without re-executing.
- Duplicate step names are auto-numbered (`step-1`, `step-1#2`, etc.).

**Spawn options:**
```typescript
await app.spawn('my-task', params, {
  maxAttempts: 10,
  retryStrategy: { kind: 'exponential', baseSeconds: 30, factor: 2, maxSeconds: 3600 },
  cancellation: { maxDuration: 86400, maxDelay: 3600 },
  headers: { traceId: '...' },
  idempotencyKey: 'unique-key',
  queue: 'high-priority',
});
```

### Worker Model

```typescript
const worker = await app.startWorker({
  concurrency: 4,          // Max parallel tasks (default: 1)
  claimTimeout: 120,        // Lease duration in seconds (default: 120)
  batchSize: 4,             // Tasks claimed per poll (default: concurrency)
  pollInterval: 0.25,       // Seconds between idle polls (default: 0.25)
  fatalOnLeaseTimeout: true, // Kill process if task exceeds 2x timeout (default: true)
  onError: (err) => {},     // Error callback
});
```

**How polling works:**
1. Worker calls `absurd.claim_task()` with `batchSize` and `claimTimeout`.
2. Claimed tasks are executed concurrently up to the `concurrency` limit.
3. Each checkpoint write automatically extends the lease.
4. If a task throws `SuspendTask` (from sleep/awaitEvent), the worker yields.
5. If a task throws `CancelledTask`, the worker acknowledges cancellation.
6. All other errors are caught, serialized, and the run is marked failed.
7. If idle, the worker waits `pollInterval` seconds before the next poll.
8. If `fatalOnLeaseTimeout` is true and a task exceeds 2× the claim timeout, the
   process exits to prevent zombie execution.

### Hooks System

```typescript
const app = new Absurd({
  hooks: {
    // Inject trace IDs or correlation headers before spawning
    beforeSpawn: (taskName, params, options) => {
      return { ...options, headers: { ...options.headers, traceId: getTraceId() } };
    },
    // Wrap task execution (e.g., restore AsyncLocalStorage context)
    wrapTaskExecution: async (ctx, execute) => {
      return asyncLocalStorage.run({ taskId: ctx.taskID }, execute);
    },
  },
});
```

---

## Python SDK

**Location:** `sdks/python/` | **Status:** Unpublished | **File:** `src/absurd_sdk/__init__.py` (~1,428 lines)

The Python SDK mirrors the TypeScript SDK's design but provides both synchronous
(`Absurd`) and asynchronous (`AsyncAbsurd`) clients using `psycopg`.

**Features:**
- Step-based checkpointing with the same caching semantics
- Event-driven suspension with timeouts
- Configurable retry strategies (fixed, exponential, none)
- Task cancellation policies
- Hooks system (before_spawn, wrap_task_execution)
- Idempotency key support
- Context variables for propagating state

**Supports:** Python 3.9–3.12

**Tests:** 8 test files covering basic execution, step caching, events, hooks, retry
logic, idempotency, and task context, using testcontainers with PostgreSQL 16.

---

## Habitat Web UI

**Location:** `habitat/` | **Stack:** Go backend + SolidJS frontend

Habitat is a monitoring dashboard that connects directly to Postgres and displays task
state, run history, checkpoint data, events, and queue metrics.

### Go Backend

**Entry point:** `cmd/habitat/main.go`

- Parses configuration from CLI flags and `HABITAT_*` environment variables
- Connects to Postgres with a 5-second ping timeout
- Serves HTTP on configurable address (default `:7890`)
- Graceful shutdown on SIGINT/SIGTERM with 5-second timeout
- Request logging middleware (method, path, status, duration)

**Configuration:**

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-listen` | `HABITAT_LISTEN` | `:7890` | Listen address |
| `-db-url` | `HABITAT_DB_URL` | — | Full Postgres URL (takes precedence) |
| `-db-host` | `HABITAT_DB_HOST` | `localhost` | Database host |
| `-db-port` | `HABITAT_DB_PORT` | `5432` | Database port |
| `-db-name` | `HABITAT_DB_NAME` | `absurd` | Database name |
| `-db-user` | `HABITAT_DB_USER` | — | Database user |
| `-db-password` | `HABITAT_DB_PASSWORD` | — | Database password |
| `-db-sslmode` | `HABITAT_DB_SSLMODE` | `disable` | SSL mode |

**Query patterns:**
- Queries `absurd.queues` for queue discovery
- Constructs per-queue table names with `pq.QuoteIdentifier()` for safe SQL
- Each query has a 5–10 second context timeout
- Uses `sql.Null*` types for nullable fields

### SolidJS Frontend

**Stack:** SolidJS 1.9 + @solidjs/router + shadcn-solid + Tailwind CSS 4 + Vite 7

The UI is built by Vite into `internal/web/dist/` and embedded in the Go binary via
`go:embed`, producing a single self-contained executable.

**Views:**

| Route | Component | Description |
|-------|-----------|-------------|
| `/` | Overview | Dashboard with queue metrics, active queues, message counts. Auto-refreshes every 15s. |
| `/tasks` | Tasks | Searchable, filterable task list with pagination (25/page). Filters by status, queue, task name, task ID. Click to expand details with checkpoints and wait states. |
| `/tasks/:taskId/runs` | TaskRuns | All runs for a specific task with expandable details. |
| `/events` | EventLog | Global event log filterable by queue and event name. JSON payload viewer. |
| `/queues` | Queues | Card grid showing all queues with per-state task counts. |

**Key components:**
- **JSONViewer** — Syntax-highlighted, collapsible JSON with copy-to-clipboard
- **TaskStatusBadge** — Color-coded status indicator
- **IdDisplay** — Truncated UUID display with copy button
- **AutoRefreshToggle** — Toggle for polling refresh

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /_healthz` | Health check (returns "ok") |
| `GET /api/metrics` | Per-queue metrics (length, age, total count) |
| `GET /api/tasks` | Task list with filtering and pagination |
| `GET /api/tasks/{runId}` | Task detail with checkpoints and wait states |
| `GET /api/queues` | Queue list with per-state task counts |
| `GET /api/queues/{name}/tasks` | Tasks for a specific queue |
| `GET /api/queues/{name}/events` | Events for a specific queue |
| `GET /api/events` | Global event log |

---

## absurdctl CLI

**Location:** `absurdctl` (single Python file, ~1,556 lines)

A command-line tool for managing queues, spawning tasks, inspecting state, and running
cleanup. Uses `psycopg` for database access and `optparse` for argument parsing.

### Commands

| Command | Description |
|---------|-------------|
| `init` | Apply `absurd.sql` schema to a database |
| `create-queue NAME` | Create a new queue |
| `drop-queue NAME` | Drop a queue (with confirmation) |
| `list-queues` | List all queues |
| `spawn-task NAME` | Spawn a task with params, headers, retry config |
| `list-tasks` | List tasks with filtering by queue/name/status |
| `cancel-task TASK_ID` | Cancel a task |
| `dump-task` | Dump detailed task/run info (by task ID or run ID) |
| `cleanup QUEUE TTL_DAYS` | Delete old tasks and events |
| `agent-help` | Print AI-agent-friendly help text |

### Parameter Syntax

```bash
# String parameter
absurdctl spawn-task my-task -P name=Alice

# JSON parameter
absurdctl spawn-task my-task -P count:=42

# Nested object
absurdctl spawn-task my-task -P user.name=Alice -P user.age:=30

# Base params with overlay
absurdctl spawn-task my-task --params '{"defaults": true}' -P override=yes
```

### Connection

Supports `PGDATABASE` (connection URL or database name), `PGHOST`, `PGPORT`, `PGUSER`,
`PGPASSWORD` environment variables, or CLI flags `-d`, `-h`, `-p`, `-U`.

---

## Test Suite

### Core SQL Tests

**Location:** `tests/` | **Framework:** pytest + testcontainers (PostgreSQL 16)

Tests the stored procedures directly via the `AbsurdTestClient` helper, which wraps
all SQL function calls as Python methods.

**Key capabilities:**
- `set_fake_now(datetime)` — Controls time via the `absurd.fake_now` session variable
- `spawn_task()`, `claim_tasks()`, `complete_run()`, `fail_run()` — Full lifecycle
- `await_event()`, `emit_event()` — Event coordination
- `get_task()`, `get_run()`, `get_checkpoint()` — State inspection

**Test files (6):**

| File | Tests |
|------|-------|
| `test_queue_management.py` | Queue CRUD, cleanup with TTL and dry-run |
| `test_checkpoints.py` | Claim extension on checkpoint, checkpoint survival across retries |
| `test_events.py` | Await/emit flow, timeout handling, lost-wakeup race condition regression |
| `test_state_transitions.py` | Sleep/wake, claim timeout recovery, event timeout wake |
| `test_retry_and_cancellation.py` | Fixed retry, max attempts, max duration, manual cancel, AB001 errors |
| `test_idempotent_spawn.py` | Idempotency key dedup, cross-name keys, post-completion dedup |

### TypeScript SDK Tests

**Location:** `sdks/typescript/test/` | **Framework:** Vitest + @testcontainers/postgresql

7 test files covering queue management, task spawning/claiming, step execution and
caching, event awaiting with timeouts, retry strategies, cancellation policies, worker
concurrency, hooks, and idempotency.

### Python SDK Tests

**Location:** `sdks/python/tests/` | **Framework:** pytest + testcontainers

8 test files covering basic execution, step caching, events, hooks, retry logic,
idempotency, and task context.

---

## Build and CI/CD

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make format` | Format TS (Prettier), Python (ruff), Go (gofmt) |
| `make test` | Run all three test suites sequentially |
| `make test-core` | Core SQL tests only |
| `make test-typescript` | TypeScript SDK tests only |
| `make test-python` | Python SDK tests only |

### GitHub Actions

**`test.yml`** — Runs on push and PR:
- Python 3.12 + uv, Node.js 20
- Executes `make test`

**`build.yml`** — Runs on tags:
- Builds Habitat for Linux x86_64, Linux ARM64, macOS ARM64
- Creates GitHub releases with binaries
- Publishes `absurd-sdk` to npm with OIDC provenance

### Schema Validation

**`scripts/validate-psql`** spins up two PostgreSQL containers via testcontainers and
verifies that applying the migration chain from an older version produces an identical
schema to applying the full `absurd.sql` from scratch — ensuring migrations stay in sync
with the canonical schema.

---

## Release Process

**`scripts/release.sh`** automates releases:

1. Bumps the semantic version (major, minor, or patch)
2. Renames migration files to match the new version
3. Validates CHANGELOG.md has an entry for the new version
4. Updates `package.json` version in the TypeScript SDK
5. Creates a git tag
6. The CI `build.yml` then builds binaries and publishes to npm

### Version History (CHANGELOG.md)

| Version | Highlights |
|---------|------------|
| 0.0.7 | Hooks support (TS), event race condition fix |
| 0.0.6 | Python SDK, idempotent spawning, Habitat improvements |
| 0.0.5 | `bindToConnection`, SSL support, heartbeat extension |
| 0.0.4 | Stuck worker termination, claim expiry retries |
| 0.0.1–0.0.3 | Initial releases with core features |

---

## State Machine Summary

```
                    ┌─────────────────────────────────┐
                    │           spawn_task()           │
                    └────────────┬────────────────────┘
                                 ▼
                            ┌─────────┐
                            │ pending │
                            └────┬────┘
                   claim_task()  │
                                 ▼
                            ┌─────────┐
                 ┌──────────│ running │──────────┐
                 │          └────┬────┘          │
                 │               │               │
          schedule_run()    complete_run()   fail_run()
          / await_event()        │               │
                 │               ▼               ▼
            ┌──────────┐   ┌───────────┐   ┌────────┐
            │ sleeping │   │ completed │   │ failed │──── retries left? ──┐
            └─────┬────┘   └───────────┘   └────────┘                     │
                  │                                                        │
        claim_task() (when                               new run created   │
        available_at reached                             (pending/sleeping) │
        or event emitted)                                                  │
                  │                                                        │
                  └──────────► running ◄──────────────────────────────────┘

        Any non-terminal state ──── cancel_task() ────► cancelled
```

Each transition is enforced by stored procedures with row-level locking to prevent
races. The claim-based leasing model means that if a worker crashes, the expired claim
triggers `fail_run()` during the next `claim_task()` call, automatically scheduling a
retry.
