# REST Catalog Scan Planning Design

Status: draft  
Owner: Revanth Ch  
Issue: https://github.com/apache/iceberg-go/issues/1178  
Last updated: 2026-06-12

## Summary

This document proposes client-side support in iceberg-go for REST Catalog
server-side scan planning. Today `(*table.Scan).PlanFiles` always plans scans
locally by reading metadata and manifest files through the client's FileIO.
The REST Catalog spec now includes scan-planning endpoints that allow a REST
server to plan a scan and return `FileScanTask` results, optionally with
plan-scoped storage credentials.

The goal is to add this capability without breaking existing users. Local
planning remains the default unless the REST table config explicitly requires
server planning (`scan-planning-mode=server`; see Scan-Planning Mode
Resolution). Otherwise remote planning is opt-in behind a small public API,
capability discovery, and an explicit scan option.

## Public API Surface

This is the complete public surface the design adds, in one place. The sections
further down are the rationale and mechanics; this block is what reviewers should
react to first. Two items here are not yet settled and need a maintainer call
before the seam lands — they are marked and tracked in Open Questions.

### `table` package

```go
type ScanPlanningMode string

const (
	ScanPlanningLocal  ScanPlanningMode = "local"  // default
	ScanPlanningRemote ScanPlanningMode = "remote"
	ScanPlanningAuto   ScanPlanningMode = "auto"
)

func WithScanPlanningMode(mode ScanPlanningMode) ScanOption

type ScanPlanner interface {
	SupportsRemoteScanPlanning() bool // can complete a remote plan end-to-end
	PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error)
}

type ScanPlanningRequest struct { /* flattened Scan inputs; see below */ }
type ScanPlanningResult struct {
	Tasks []FileScanTask
	IO    FSysF // PROVISIONAL carrier — unsettled, see Open Question 1
}

// New field on the existing FileScanTask:
//   Residual iceberg.BooleanExpression
```

### root `iceberg` package

```go
func MarshalExpressionJSON(expr BooleanExpression) ([]byte, error)
func UnmarshalExpressionJSON(data []byte, schema *Schema) (BooleanExpression, error)
```

### `catalog/rest` package

```go
type Endpoint string // POST .../plan, GET/DELETE .../plan/{id}, POST .../tasks

func (c *Catalog) SupportsEndpoint(ep Endpoint) bool
func (c *Catalog) SupportsPlanTableScan() bool          // plan endpoint only
func (c *Catalog) SupportsFullRemoteScanPlanning() bool // all four endpoints

func (c *Catalog) PlanTableScan(ctx context.Context, ident table.Identifier, req PlanTableScanRequest) (PlanTableScanResponse, error)
func (c *Catalog) FetchPlanningResult(ctx context.Context, ident table.Identifier, planID string) (FetchPlanningResultResponse, error)
func (c *Catalog) CancelPlanning(ctx context.Context, ident table.Identifier, planID string) error
func (c *Catalog) FetchScanTasks(ctx context.Context, ident table.Identifier, req FetchScanTasksRequest) (FetchScanTasksResponse, error)
func (c *Catalog) WaitForPlan(ctx context.Context, ident table.Identifier, planID string, opts WaitForPlanOptions) (FetchPlanningResultResponse, error)

var ErrPlanExpired = fmt.Errorf("%w: scan plan expired", ErrRESTError)
```

`rest.Catalog` implements `table.ScanPlanner`. The two unsettled surface
decisions are (1) how plan-scoped FileIO reaches `ReadTasks` across the
`PlanFiles` -> `ReadTasks` boundary, and (2) how the capability gate is split.
Both are open design questions — see Open Questions.

## Background

Large Iceberg tables can have substantial metadata and manifest state. Planning
those scans entirely in a short-lived Go client can be expensive, slow, or
impossible in environments like Lambda, Cloud Run, and small Kubernetes jobs.
Server-side planning lets a REST catalog perform planning near catalog metadata
and return only the scan tasks needed by the client.

This also enables catalog-side governance and per-scan vended storage
credentials. The REST server can apply table policies, time-travel restrictions,
or row/column governance during planning, and the client can receive temporary
credentials scoped to the files returned by that planning result.

The Java implementation is the primary reference. The most important reference
behaviors are:

- `PlanTableScanRequest` validates snapshot ID vs incremental snapshot IDs.
- Expression JSON uses Java's `ExpressionParser` format.
- Scan-task responses include `delete-files`, `file-scan-tasks`,
  `plan-tasks`, optional `storage-credentials`, and optional task residuals.
- Async planning returns a `plan-id` that must be polled.
- Clients should cancel plans that are no longer needed.

## Goals

- Add capability discovery for REST scan-planning endpoints.
- Add Go wire types for the REST scan-planning request and response payloads.
- Add an expression JSON codec compatible with Java's `ExpressionParser`.
- Add REST client methods for the four scan-planning endpoints.
- Add a table-level planner seam so `table` does not import `catalog/rest`.
- Add opt-in remote planning for `(*table.Scan).PlanFiles`.
- Preserve default local planning behavior.
- Provide tests proving local-vs-remote planning parity.

## Non-Goals

- Implementing a REST server.
- Building a distributed query engine or worker runtime.
- Replacing or refactoring the local planning path.
- Making remote planning the default before interop is proven.
- Requiring non-REST catalogs to implement scan planning.

## REST Spec Surface

The relevant REST endpoints are:

- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`
- `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
- `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks`

The relevant request and response shapes are:

- `PlanTableScanRequest`
- `PlanTableScanResult`
- `FetchPlanningResult`
- `FetchScanTasksRequest`
- `FetchScanTasksResult`
- `PlanStatus`
- `PlanTask`
- `StorageCredential`
- REST `FileScanTask`

The catalog `/v1/config` response may include an `endpoints` array. If omitted,
the REST spec assumes a default endpoint set. That default set does not include
scan planning, so scan planning must be treated as supported only when the
server explicitly advertises the relevant endpoints.

The table load config may include `scan-planning-mode`:

- `client`: clients must use client-side scan planning
- `server`: clients must use server-side scan planning through `planTableScan`

This is a server/client compatibility signal. It should not be confused with
the user-facing scan option proposed below.

## Current iceberg-go State

Relevant local code:

- `catalog/rest/rest.go`
  - `configResponse` currently decodes defaults and overrides only.
  - `Catalog` does not store endpoint capabilities.
  - `LoadTable` creates `table.Table` using `table.New`.

- `table/table.go`
  - `Table` stores metadata, metadata location, catalog IO, and FileIO factory.
  - `ScanOption` already provides the extension point for scan settings.
  - `Table.Scan` creates a `Scan` with local metadata and FileIO.

- `table/scanner.go`
  - `Scan.PlanFiles` always performs local planning.
  - It reads manifests, matches positional deletes, equality deletes, and
    deletion vectors.
  - `FileScanTask` has data file, delete files, range, and row-lineage fields.
  - `FileScanTask` does not currently carry a residual filter.

- `codec/file_scan_task.go`
  - Encodes `table.FileScanTask` for cross-process transport using Avro bytes.
  - Has a compile-time drift guard that will fail if `FileScanTask` changes.

## Design Principles

- Keep the default path local and unchanged.
- Keep `table` independent from `catalog/rest`.
- Make low-level REST client methods available for advanced users.
- Keep the public API small at first.
- Land the work in reviewable phases.
- Treat expression JSON compatibility as a correctness-critical component.
- Avoid silently falling back from requested remote planning to local planning.

## Proposed Public API

Add a scan-planning mode to the `table` package:

```go
package table

type ScanPlanningMode string

const (
	ScanPlanningLocal  ScanPlanningMode = "local"
	ScanPlanningRemote ScanPlanningMode = "remote"
	ScanPlanningAuto   ScanPlanningMode = "auto"
)

func WithScanPlanningMode(mode ScanPlanningMode) ScanOption
```

Behavior:

- `local`: always use current local planning.
- `remote`: require a planner and advertised remote capability; fail loudly if
  remote planning is unavailable.
- `auto`: use remote planning when available and allowed; otherwise use local.

Local should remain the default.

Add a planner seam in `table`:

```go
package table

type ScanPlanningRequest struct {
	Identifier        Identifier
	Metadata          Metadata
	MetadataLocation  string
	SnapshotID        *int64
	StartSnapshotID   *int64
	EndSnapshotID     *int64
	SelectedFields    []string
	RowFilter         iceberg.BooleanExpression
	CaseSensitive     bool
	UseSnapshotSchema bool
	MinRowsRequested  *int64
	StatsFields       []string
}

type ScanPlanningResult struct {
	Tasks []FileScanTask
	IO    FSysF
}

type ScanPlanner interface {
	SupportsRemoteScanPlanning() bool
	PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error)
}
```

`ScanPlanner.SupportsRemoteScanPlanning` reports whether the planner can complete
a remote plan end-to-end for the requested scan; `rest.Catalog` backs it with the
split capability checks (`SupportsPlanTableScan` / `SupportsFullRemoteScanPlanning`,
depending on the response the server returns).

`ScanPlanningResult.IO` is optional. If set, `ReadTasks` should use it for files
returned by this plan. If nil, the table's existing FileIO factory is used.
Note: the carrier for plan-scoped FileIO is an open surface decision (Open
Question 1) — `ScanPlanningResult.IO` is one option among several, and a live
FileIO should not sit on `FileScanTask`. See Scanner Delegation for the options.

This keeps `StorageCredential` out of the `table` package. REST-specific
credentials stay in `catalog/rest`, where they can be resolved into a FileIO
factory.

## REST Package API

Add endpoint constants and capability checks:

```go
package rest

type Endpoint string

const (
	EndpointPlanTableScan       Endpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
	EndpointFetchPlanningResult Endpoint = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
	EndpointCancelPlanning      Endpoint = "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
	EndpointFetchScanTasks      Endpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks"
)

func (c *Catalog) SupportsEndpoint(endpoint Endpoint) bool
func (c *Catalog) SupportsPlanTableScan() bool         // plan endpoint only
func (c *Catalog) SupportsFullRemoteScanPlanning() bool // all four endpoints
```

**Surface decision (needs a maintainer call).** A single
`SupportsRemoteScanPlanning` gate is too coarse. Requiring all four endpoints
makes `auto` fall back to local against any sync-only server; requiring only the
`plan` endpoint false-positives, because `planTableScan` can return `submitted`
or `plan-tasks`, which need the poll/fetch endpoints. Recommendation: split into
`SupportsPlanTableScan()` (plan endpoint only) and
`SupportsFullRemoteScanPlanning()` (all four). With only `SupportsPlanTableScan`,
remote planning is valid solely for a `completed` inline response; if the server
returns `submitted` or `plan-tasks` and the required poll/fetch endpoints are not
advertised, that is an error, not a silent fallback to local. The full
async/fanout path requires `SupportsFullRemoteScanPlanning`. Confirm what the
Java `iceberg-rest-fixture` actually advertises before locking this in.

Add low-level client methods:

```go
func (c *Catalog) PlanTableScan(
	ctx context.Context,
	ident table.Identifier,
	req PlanTableScanRequest,
) (PlanTableScanResponse, error)

func (c *Catalog) FetchPlanningResult(
	ctx context.Context,
	ident table.Identifier,
	planID string,
) (FetchPlanningResultResponse, error)

func (c *Catalog) CancelPlanning(
	ctx context.Context,
	ident table.Identifier,
	planID string,
) error

func (c *Catalog) FetchScanTasks(
	ctx context.Context,
	ident table.Identifier,
	req FetchScanTasksRequest,
) (FetchScanTasksResponse, error)
```

Add a higher-level helper:

```go
func (c *Catalog) WaitForPlan(
	ctx context.Context,
	ident table.Identifier,
	planID string,
	opts WaitForPlanOptions,
) (FetchPlanningResultResponse, error)
```

`rest.Catalog` should implement `table.ScanPlanner`.

## Scan-Planning Mode Resolution

Suggested behavior:

| User mode        | Catalog capability | Table config      | Behavior                                         |
| ---------------- | ------------------ | ----------------- | ------------------------------------------------ |
| default          | any                | absent / `client` | local planning                                   |
| default          | supported          | `server`          | remote planning                                  |
| default          | unsupported        | `server`          | error: server requires remote, client cannot     |
| local (explicit) | any                | absent / `client` | local planning                                   |
| local (explicit) | any                | `server`          | error: user forced local, server requires remote |
| remote           | unsupported        | any               | error                                            |
| remote           | supported          | `client`          | error                                            |
| remote           | supported          | absent / `server` | remote planning                                  |
| auto             | unsupported        | any               | local planning                                   |
| auto             | supported          | `client`          | local planning                                   |
| auto             | supported          | `server` / absent | remote planning                                  |

The table reflects a proposed fail-fast contract for `scan-planning-mode=server`:
config `server` requires remote planning, and an explicit `ScanPlanningLocal`
against a `server` table is an error rather than a silent local plan. This
follows the REST spec's "clients must use server-side planning" for `server`, at
the cost of possibly surprising users who didn't ask for remote. This is an open
question — confirm the reading with maintainers before scanner integration; see
Open Question 4.

## Expression JSON Codec

Expression JSON compatibility is the highest-risk part of the project.

The codec should live in the root `iceberg` package because expression internals
are defined there and some needed fields are not exported outside the package.
This avoids adding many public accessors solely for REST serialization.

Proposed API:

```go
package iceberg

func MarshalExpressionJSON(expr BooleanExpression) ([]byte, error)
func UnmarshalExpressionJSON(data []byte, schema *Schema) (BooleanExpression, error)
```

The JSON format follows Java's `ExpressionParser`. The `true`/`false`, `not`,
`and`/`or`, and predicate field names below are confirmed against the Java
source and `TestExpressionParser` golden strings; check the fixtures in for
regression safety and confirm the literal/transform encodings the same way.

- `AlwaysTrue` -> bare JSON `true`
- `AlwaysFalse` -> bare JSON `false`
- `not` -> object with `type` and `child`
- `and` / `or` -> object with `type`, `left`, and `right`
- predicates -> object with `type`, `term`, and optional `value` or `values`
- references -> string name, with object reference accepted during parse
- transforms -> object with `type: "transform"`, `transform`, and `term`

(`ExpressionParser` also accepts an explicit `{"type":"literal","value":...}`
object on parse, but serialization always emits the compact form, including the
bare boolean for `true`/`false`.)

Operations to cover:

- `is-null`
- `not-null`
- `is-nan`
- `not-nan`
- `lt`
- `lt-eq`
- `gt`
- `gt-eq`
- `eq`
- `not-eq`
- `starts-with`
- `not-starts-with`
- `in`
- `not-in`
- `and`
- `or`
- `not`

Literal encoding must match Iceberg primitive JSON encodings:

- boolean as JSON boolean
- int/long as JSON numbers
- float/double as JSON numbers
- date as ISO date string when schema-bound
- time as ISO time string when schema-bound
- timestamp/timestamptz as ISO timestamp strings when schema-bound
- uuid as string
- decimal as string
- fixed/binary as uppercase hex string

Unbound expressions have less type information. Java handles common unbound
literal values directly, but full date/time/decimal fidelity requires schema
context. The initial implementation should prioritize schema-aware round trips
for REST response residuals and Java golden fixtures.

## REST Content-File JSON

REST scan-planning responses use JSON `DataFile` and `DeleteFile` objects, not
the existing Avro/binary task codec in `codec/file_scan_task.go`.

Add REST JSON helpers in `catalog/rest`, or in an internal package under it, for:

- `DataFile`
- `DeleteFile`
- REST `FileScanTask`
- `ScanTasks`

The parser needs table metadata to map `spec-id` to partition specs and schemas.
REST content-file JSON stores partition values as an ordered list following the
partition spec fields. The Go `DataFileBuilder` expects partition data keyed by
partition field ID, so decoding must convert ordered partition arrays into the
field-ID keyed map.

Delete files returned in REST `delete-files` need to be classified when building
Go `table.FileScanTask` values:

- `content=position-deletes` and `file-format=puffin` should map to
  `DeletionVectorFiles`.
- `content=position-deletes` and non-puffin format should map to `DeleteFiles`.
- `content=equality-deletes` should map to `EqualityDeleteFiles`.

This classification belongs in the REST scan-task decoder.

## FileScanTask Residuals

REST `FileScanTask` may include `residual-filter`. The REST spec says that if
the residual is absent, the client must produce the residual or use the original
filter.

Go currently has no residual field:

```go
type FileScanTask struct {
	File                iceberg.DataFile
	DeleteFiles         []iceberg.DataFile
	EqualityDeleteFiles []iceberg.DataFile
	DeletionVectorFiles []iceberg.DataFile
	Start, Length       int64
	FirstRowID          *int64
	DataSequenceNumber  *int64
}
```

Proposed addition:

```go
Residual iceberg.BooleanExpression
```

Reading behavior:

- If `task.Residual` is set, apply it for that task.
- If `task.Residual` is nil, keep current behavior and apply the scan-level
  filter.

Local planning can initially leave `Residual` nil, preserving current behavior.
Remote planning should set `Residual` when the server returns it.

This change requires updating:

- `table.FileScanTask`
- `table.ReadTasks` / `arrowScan` filter handling
- `codec/file_scan_task.go` envelope and drift guard
- tests that compare task equality

## Schema Evolution Across the Boundary

(This is the epic's open question 4 and the subtle silent-corruption risk.)

Remote planning splits responsibility across two processes that may hold
different schema views:

- The server plans against the scan's snapshot schema and partition specs.
- REST content-file JSON returns partition values as an ordered list following
  the spec fields for each file's `spec-id`, not keyed by field ID.
- The client projects against the schema it resolves in `Scan.Projection`, which
  can differ if the table evolved between the planned snapshot and "current".

The design must pin, explicitly:

- Which schema binds a returned `residual-filter`. It must be the schema the
  server planned against — the snapshot's schema, resolved via the snapshot's
  schema-id — not the client's current schema, or field references can bind to
  the wrong field. This is a _schema_ question (schema-id / snapshot) and is
  independent of partition spec-id below.
- Which partition spec drives the ordered-partition-array -> field-ID-map decode.
  It must be the spec named by each file's `spec-id`, looked up from table
  metadata, not the current spec. `spec-id` selects the partition spec only; it
  does not carry the schema.

`ScanPlanningRequest.UseSnapshotSchema` is the current placeholder for selecting
this binding; it needs a concrete contract, not a bare boolean. This is the case
the parity tests must cover with an actually-evolved table, and a question for
maintainers.

## Storage Credentials

REST planning responses may return `storage-credentials`. The server expects the
client to use them to read files returned by the planning result.

Existing iceberg-go vended credential code resolves the longest-prefix credential
for a table metadata location and builds a FileIO. For scan planning, credentials
are plan-scoped and may apply to data/delete file prefixes, not only metadata.

Initial design:

- Keep `storageCredential` internal to `catalog/rest`.
- Resolve returned credentials into an optional `table.FSysF`.
- If one credential prefix clearly covers the table location or all returned
  files, build a plan-scoped FileIO from it.
- If multiple credentials are returned, use longest-prefix matching to select
  config for each file prefix.

Open implementation choice:

- MVP can support the common single-prefix case first.
- Full support may require a routing FileIO that dispatches `Open` by file path
  prefix to different underlying FileIO instances.

Because credentials are security-sensitive, remote planning should not ignore
returned storage credentials when present.

## Polling, Cancellation, and Errors

`PlanTableScan` can return:

- `completed`: includes inline scan tasks and/or plan tasks.
- `submitted`: includes `plan-id`; client must poll.
- `failed`: includes REST error.

`FetchPlanningResult` can return:

- `completed`
- `submitted`
- `cancelled`
- `failed`

`CancelPlanning` should be called when:

- the context is cancelled while polling;
- the caller stops before all plan tasks are fetched;
- remote planning fails after a server-side plan was created.

Error behavior:

- 400/403 should surface as typed REST errors.
- 404 while polling should map to a sentinel such as `ErrPlanExpired`.
- 503 should be retried with backoff.
- context cancellation should attempt `CancelPlanning`, then return
  `context.Canceled` or the context error.

Add:

```go
var ErrPlanExpired = fmt.Errorf("%w: scan plan expired", ErrRESTError)
```

Polling should use jittered exponential backoff with configurable min/max delay
and timeout. Defaults should be conservative.

## Scanner Delegation

`Scan.PlanFiles` should remain the single public entry point.

Pseudo-flow:

```text
PlanFiles(ctx):
  resolve snapshot/as-of timestamp
  resolve scan planning mode

  if mode is local:
    return localPlanFiles(ctx)

  if mode is remote:
    require planner
    require planner.SupportsRemoteScanPlanning()
    return remotePlanFiles(ctx)

  if mode is auto:
    if planner supports remote and table config allows it:
      return remotePlanFiles(ctx)
    return localPlanFiles(ctx)
```

Remote planning flow:

```text
build ScanPlanningRequest from Scan
call planner.PlanFiles(ctx, req)
if result.IO is set, preserve it for ReadTasks
return result.Tasks
```

**Surface decision (needs a maintainer call).** `PlanFiles` returns only
`[]FileScanTask`, and `ReadTasks(ctx, tasks)` is a separate public call. A
plan-scoped FileIO carried only on `ScanPlanningResult` (or stashed on the
`Scan`) cannot reach `ReadTasks` for any caller who runs the two steps manually —
they would silently read data files with the table's default credentials, which
may be exactly the ones without access. Stashing it on the `Scan` receiver works
only because `PlanFiles` already mutates the receiver during snapshot
resolution, which is a latent race we should not lean on harder.

The boundary problem is real, but the carrier is an open design choice — and a
_live_ FileIO should not go on `FileScanTask`, which is a serializable data value
with a transport codec (`codec/file_scan_task.go`); putting runtime IO state on
it mixes concerns and breaks task serialization. Options to put to maintainers,
not a pre-made pick:

- a `ScanPlan` / planned-result object owning both the tasks and the plan-scoped
  IO, returned by a new entry point (largest API change);
- an internal plan context the `Scan` holds between `PlanFiles` and `ReadTasks`
  (smallest; does not survive a manual two-step across `Scan` instances);
- a serializable _credential handle_ (not a live IO) on `FileScanTask`, resolved
  to a FileIO at read time (keeps the task serializable, still widens it).

This shapes `FileScanTask` and possibly the codec, so it must be decided before
the seam lands (PR 6).

## Incremental Scans

The REST request supports:

- point-in-time scans with `snapshot-id`
- incremental scans with `start-snapshot-id` and `end-snapshot-id`

iceberg-go already supports `WithSnapshotID` and `WithSnapshotAsOf`, but does
not currently expose an incremental scan option in `table.Scan`.

Initial implementation should support point-in-time remote scans first. Add
incremental scan options in a later phase unless maintainers want them in the
initial API.

Possible future API:

```go
func WithSnapshotRange(startExclusive, endInclusive int64) ScanOption
```

## Test Strategy

### Unit Tests

Capability discovery:

- config response with no `endpoints`
- config response with default endpoints only
- config response with all scan-planning endpoints
- malformed or duplicate endpoints
- `SupportsPlanTableScan` / `SupportsFullRemoteScanPlanning`

Wire types:

- request validation
- JSON field names
- snapshot ID XOR incremental IDs
- negative `min-rows-requested`
- status-specific response validation

Expression JSON:

- Java golden JSON for every operation
- Java golden JSON for primitive literal types
- bound and unbound expression round trips
- schema-aware parse for date/time/timestamp/decimal/fixed/binary
- invalid expression JSON cases

Content-file JSON:

- data file parse
- positional delete parse
- equality delete parse
- partition data ordering to field ID map
- delete-file reference validation
- residual filter parse

REST client:

- endpoint path construction
- error mapping
- 404 plan expired
- 503 retry
- context cancellation calls cancel endpoint

### Fake Server Tests

Add an in-process fake REST scan-planning server with switches for:

- sync completed result
- async submitted then completed result
- plan task fanout
- failed planning
- cancelled planning
- expired plan ID
- storage credentials

The fake server may use local planning internally, but be explicit about what
that proves. A fake that plans locally, compared against local planning, is
tautological: it exercises plumbing (serialization, polling, task decoding), not
planning correctness. It cannot catch the two failure modes that make remote
planning risky — an `ExpressionParser`-incompatible filter, or a wrong
server-supplied residual — because the fake reuses our own correct planner to
produce both sides. Use the fake for plumbing and the failure-path switches
above; use the Java fixture (below) as the correctness oracle.

### Parity Tests

For the same table, snapshot, projection, and filter:

- local mode task set must equal remote mode task set
- compare data file path
- compare start and length
- compare positional delete files
- compare equality delete files
- compare deletion vector files when included
- compare residual filters

### Integration Tests

Use the existing local REST fixture for negative capability tests. The local
fixture currently does not advertise scan-planning endpoints, so it is useful
for proving `auto` fallback and `remote` fail-fast behavior.

Add a gated integration suite against the Java `iceberg-rest-fixture` and check
in golden request/response fixtures captured from it. This is the correctness
oracle for the local-vs-remote parity acceptance criterion; the in-process fake
proves plumbing only. First confirm whether the standard fixture implements the
plan endpoints, and whether it supports only synchronous planning or async too —
that answer also drives the capability-gating decision above.

## Phased PR Plan

### PR 1: Capability Discovery

Scope:

- Extend `configResponse` to decode `endpoints []string`.
- Store endpoint capabilities on `rest.Catalog`.
- Add endpoint constants.
- Add `SupportsEndpoint`.
- Add `SupportsPlanTableScan` and `SupportsFullRemoteScanPlanning` (Open Question 2).
- Unit tests.

Why first:

- Small, reviewable, no behavior change.
- Creates the foundation for `auto` mode.

### PR 2: Expression JSON Codec

Scope:

- Add expression JSON marshal/unmarshal in root `iceberg`.
- Match Java `ExpressionParser`.
- Add golden tests from Java.

Why second:

- It is the highest correctness risk.
- Remote filtering is unsafe without this.

### PR 3: REST Content-File and Scan-Task JSON

Scope:

- Decode REST `DataFile`.
- Decode REST `DeleteFile`.
- Decode REST `FileScanTask`.
- Decode `delete-file-references`.
- Decode `residual-filter`.
- Add `Residual` to `table.FileScanTask`.
- Update task codec drift guard.

Why third:

- REST planning responses cannot be consumed without this.

### PR 4: Wire Types

Scope:

- Add `PlanTableScanRequest`.
- Add `FetchScanTasksRequest`.
- Add planning response types.
- Add `PlanStatus`.
- Add `PlanTask`.
- Add validation.
- Add JSON tests.

Why after codecs:

- Response types depend on expression and scan-task decoding.

### PR 5: REST Client Methods and Poller

Scope:

- Add four REST methods.
- Add `WaitForPlan`.
- Add retry/backoff.
- Add cancel-on-context-cancel.
- Add `ErrPlanExpired`.
- Add tests with `httptest`.

Why here:

- Low-level REST support is complete before table integration.

### PR 6: Table Planner Seam

Scope:

- Add `table.ScanPlanner`.
- Add planner field to `Table`.
- Add constructor path for planner.
- Wire `rest.Catalog` as planner when loading tables.
- Keep non-REST catalogs nil.

Why separate:

- This is the main public API design point.

### PR 7: Scanner Delegation

Scope:

- Add `WithScanPlanningMode`.
- Add local/remote/auto resolution.
- Branch in `PlanFiles`.
- Materialize plan tasks through `FetchScanTasks`.
- Use plan-scoped FileIO when returned.

Why separate:

- This is the behavior-changing value-delivery PR.

### PR 8: Fake Server and Parity Tests

Scope:

- Add in-process fake planning server.
- Add local-vs-remote parity tests.
- Add async/fanout/cancel tests.

Why after scanner delegation:

- Proves the feature is correct end to end.

### PR 9: Docs and Hardening

Scope:

- User docs.
- CLI examples.
- Feature status updates.
- Optional metrics/OTEL follow-up.
- Gated Java fixture integration.

## Acceptance Criteria

The epic is complete when:

- REST endpoint capabilities are decoded from `/v1/config`.
- iceberg-go can serialize and parse REST scan-planning expressions.
- iceberg-go can parse REST scan-task responses.
- REST catalog clients can call scan-planning endpoints directly.
- `Table.Scan(table.WithScanPlanningMode(table.ScanPlanningRemote)).PlanFiles`
  uses server-side planning when available.
- `ScanPlanningAuto` falls back to local only when remote is unavailable or not
  allowed.
- Requested remote planning fails loudly when unavailable.
- Local-vs-remote parity tests pass for representative scans.
- Context cancellation attempts to cancel the server-side plan.
- Plan-scoped storage credentials are honored when returned.

## Risks

Expression JSON mismatch:

Silent result corruption is possible if the client sends a different filter
than local planning would use. This is the highest-risk area and needs Java
golden tests.

Residual filter mismatch:

If residual filters are ignored or applied incorrectly, rows may be over-read or
under-read. `FileScanTask.Residual` support should be explicit.

Storage credential routing:

Multiple returned storage credentials may require path-prefix routing. MVP
single-prefix support may not be enough for all vendors.

Deletion vectors:

REST content-file JSON and Go's DV requirements need careful verification,
especially around `referenced-data-file` metadata. If this is not fully
represented by the REST payload, DV support should be scoped separately.

Public API churn:

The planner seam should be reviewed before scanner delegation lands. Moving it
later would be painful.

## Open Questions

1. **Plan-scoped FileIO threading (surface decision).** How does plan-scoped IO
   reach `ReadTasks` across the `PlanFiles` -> `ReadTasks` boundary? Options: a
   `ScanPlan`/planned-result object, an internal plan context on `Scan`, or a
   serializable credential handle on `FileScanTask`. No firm recommendation yet,
   but a _live_ FileIO should not go on `FileScanTask` — it is a data value with
   a transport codec. See Scanner Delegation. Must be settled before PR 6.

2. **Capability gating (surface decision).** A single gate is too coarse:
   all-four falls back to local against sync-only servers; plan-endpoint-only
   false-positives, because `planTableScan` can return `submitted`/`plan-tasks`
   needing the poll/fetch endpoints. Recommendation: split into
   `SupportsPlanTableScan()` (plan only) and `SupportsFullRemoteScanPlanning()`
   (all four). Confirm what the Java `iceberg-rest-fixture` advertises.

3. **Schema evolution across the boundary (epic open question 4).** Which schema
   binds a returned residual, and which spec binds the ordered-partition decode,
   when the planned-snapshot schema differs from the client's current schema? See
   "Schema Evolution Across the Boundary". `UseSnapshotSchema` needs a real
   contract.

4. How should `scan-planning-mode=server` be honored? The REST spec says
   `server` means clients _must_ use server-side planning, so silently ignoring
   it likely won't pass review. Proposed contract: config absent/`client` keeps
   the default local; config `server` requires remote planning, and if the client
   cannot or is configured not to (e.g. explicit `ScanPlanningLocal`), it fails
   fast rather than silently local-planning. Confirm this reading. (The
   counter-concern is surprising users by changing default scan behavior from a
   server flag.)

5. Should `table.ScanPlanningResult` expose storage credentials, or should REST
   convert them into a FileIO override internally? Recommendation: keep
   `StorageCredential` out of `table`; REST resolves to a FileIO.

6. Should expression JSON APIs be exported from `iceberg`, or kept internal until
   another package needs them?

7. Should deletion vectors be part of the first remote-planning acceptance test?

8. Should incremental scans be in the first public API, or deferred until
   point-in-time scans work? Note: the epic lists incremental CDC as a motivating
   use case, so deferring it drops a stated driver from the first cut — flag this
   to maintainers explicitly rather than letting it be implicit.

## Discussion Proposal

Before implementing beyond PR 1, ask questions to confirm:

- How plan-scoped FileIO reaches `ReadTasks` (Open Question 1) — blocks the seam
  shape; a live IO should not live on `FileScanTask`.
- Splitting the capability gate into `SupportsPlanTableScan` and
  `SupportsFullRemoteScanPlanning` (Open Question 2).
- Which schema (snapshot schema, via schema-id) binds residuals, kept separate
  from which partition spec (via spec-id) drives partition decode
  (Open Question 3).
- The fail-fast contract for `scan-planning-mode=server` (Open Question 4).
- `ScanPlanner` should live in `table`.
- User-facing modes should be `local`, `remote`, and `auto`.
- Root-package expression JSON API is acceptable.
- `FileScanTask.Residual` is acceptable.
- Single-prefix storage credential support is acceptable for the first pass, or
  routing FileIO is required.
- Incremental scans can be deferred.

## References

- iceberg-go issue: https://github.com/apache/iceberg-go/issues/1178
- REST OpenAPI: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- Java `PlanTableScanRequest`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/requests/PlanTableScanRequest.java
- Java `PlanTableScanRequestParser`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/requests/PlanTableScanRequestParser.java
- Java `PlanTableScanResponse`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/responses/PlanTableScanResponse.java
- Java `FetchPlanningResultResponse`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/responses/FetchPlanningResultResponse.java
- Java `FetchScanTasksResponse`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/responses/FetchScanTasksResponse.java
- Java `ExpressionParser`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/expressions/ExpressionParser.java
- Java `RESTFileScanTaskParser`: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/RESTFileScanTaskParser.java
