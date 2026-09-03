# Threadmark: Durable Reasoning Graphs for Agents

**Document status:** Implementation specification
**Version:** 0.1
**Working name:** Threadmark
**Primary implementation language:** Rust
**Primary interface:** CLI and MCP server
**Primary storage:** SQLite

## 1. Executive summary

Threadmark is a local-first reasoning and decision runtime for humans and AI agents working on uncertain, long-running projects.

Most planning systems model work as a list or dependency graph of tasks. Threadmark instead models the uncertainty that must be resolved before reliable execution is possible. It stores questions, decisions, assumptions, evidence, experiments, observations, and constraints as a typed graph. It then derives the current research frontier, identifies decisions whose premises have been invalidated, surfaces contradictions, and determines whether an effort is ready to hand off for specification or implementation.

Threadmark is inspired by Matt Pocock's Wayfinder method and the machine-checkable local planning format in Wayfinder Maps. It deliberately goes further in four areas:

1. Typed semantic relationships instead of a single generic blocking edge.
2. First-class assumptions, evidence, provenance, alternatives, and confidence.
3. Deterministic invalidation propagation and convergence checks.
4. Agent-facing operations for safely claiming, investigating, resolving, and revisiting parts of the graph.

The core runtime must be deterministic and model-independent. It must not require an LLM to calculate state, validate invariants, choose the frontier, propagate invalidation, or determine whether explicit exit criteria pass. LLMs may propose graph changes, extract claims, identify possible contradictions, and suggest newly visible questions, but those suggestions enter the graph through explicit, auditable operations.

The first release is a single-host, multi-session system. It supports concurrent local agents through SQLite transactions and expiring claims. Distributed multi-host synchronization, built-in agent spawning, a hosted service, and an execution-task orchestrator are intentionally deferred.

## 2. Working name

### 2.1 Recommendation: Threadmark

**Threadmark** is the recommended working name.

The name describes the product without reducing it to project management:

- A **thread** is a developing line of inquiry or reasoning.
- A **mark** is a durable, inspectable record of a question, premise, observation, choice, or revision.
- Branches and joins in the thread naturally suggest a graph.
- The name works for both the product and the CLI: `threadmark`.

Suggested tagline:

> Durable reasoning graphs for humans and agents.

Short description:

> Threadmark records what a project knows, what it assumes, what it has decided, and what must be reconsidered when the evidence changes.

This name has only undergone a cursory collision search. Repository, package, domain, and trademark availability must be checked before a public launch.

### 2.2 Other viable names

| Name | Strength | Concern |
|---|---|---|
| Stemma | Evokes lineage, provenance, and revision history | Existing technology and company usage, including Adafruit STEMMA |
| Palinode | Specifically evokes retracting and revising a conclusion | Memorable but obscure and difficult to spell |
| Warrant | In argumentation, connects evidence to a claim | Heavy legal/security connotations and likely collisions |
| Fogline | Evokes the edge of what is currently knowable | Existing product usage and emphasizes discovery more than memory |
| Lodestar | A guide toward a destination | Already heavily used in software |

Unless renamed before implementation, all packages, commands, schemas, and examples should use `threadmark`.

## 3. Problem statement

Long-running agent work fails in predictable ways:

- A task list is created before the problem is understood.
- Decisions are stored as prose without their rejected alternatives.
- Assumptions remain implicit and cannot be challenged mechanically.
- Research findings have weak or missing provenance.
- A later finding invalidates an earlier premise, but downstream decisions continue to appear settled.
- Separate agent sessions duplicate work or claim the same question.
- Two research agents reach conflicting conclusions without creating a reconciliation step.
- Agents declare planning complete because the document looks substantial, not because explicit readiness conditions have been met.
- Context is loaded as a large document rather than retrieved at the level needed for the current decision.

Issue trackers and execution DAGs are useful after the work is understood. They are poor representations of the process by which an uncertain goal becomes a defensible plan.

Threadmark addresses this by making the project's explicit reasoning state the durable artifact.

## 4. Product goals

Threadmark must:

1. Represent an uncertain effort as a typed reasoning graph.
2. Preserve decisions, alternatives, assumptions, evidence, and provenance.
3. Calculate the current actionable frontier deterministically.
4. Support safe concurrent claims by multiple local sessions or agents.
5. Detect graph inconsistencies, dangling references, cycles, and invalid state combinations.
6. Propagate the consequences of explicitly invalidated premises without claiming that downstream conclusions are automatically false.
7. Preserve the full history of decisions being resolved, challenged, reopened, and revised.
8. Express readiness as explicit pass/fail criteria rather than an invented percentage.
9. Provide compact context by default and detail on demand.
10. Remain useful without any built-in model provider.
11. Offer both human-friendly CLI output and stable machine-readable JSON/MCP interfaces.
12. Allow a converged graph to be exported as a handoff document for a specification or execution planner.

## 5. Non-goals for the first release

Threadmark v1 will not:

- Implement product or code changes represented by the graph.
- Replace GitHub Issues, Jira, Linear, or an execution task queue.
- Spawn or pay for model invocations itself.
- Store hidden chain-of-thought or raw model reasoning.
- Decide whether evidence is true solely because an LLM says so.
- Automatically invalidate an assumption merely because a contradiction is suspected.
- Synchronize writable state across multiple machines.
- Provide a collaborative hosted service.
- Build a general-purpose ontology or arbitrary knowledge graph database.
- Perform semantic/vector search in the core release.
- Crawl URLs or execute code referenced by evidence.
- Produce a large web application. A read-only graph viewer is optional after the CLI and MCP surfaces are stable.

## 6. Design principles

### 6.1 Model uncertainty, not premature work

The graph describes what must be learned or decided before execution is safe. It must not silently turn every uncertain area into implementation tasks.

### 6.2 Only map what is currently specifiable

Threadmark preserves Wayfinder's fog-of-war principle. A suspected future area remains a fog patch until its question can be stated precisely. Resolving one node may reveal new nodes; this is expected and is not scope creep by itself.

### 6.3 Structure facts; leave explanations as prose

Node kinds, states, confidence, reversibility, edges, claims, and provenance are machine-checkable. Explanations, answers, and rationales remain prose. The runtime must not parse prose to recover facts it should have stored structurally.

### 6.4 Deterministic core, probabilistic assistant

Graph operations and derived state are deterministic. Model output is a proposal. The boundary must be obvious in both APIs and UI.

### 6.5 Never confuse unsupported with false

If an assumption becomes invalid, a dependent decision becomes **undermined**, not automatically invalid. If an upstream decision changes, a transitive dependent becomes **review required**, not automatically wrong.

### 6.6 Confidence is ordinal

Threadmark uses `tentative`, `supported`, and `strong`, with a prose justification. It must not manufacture decimal confidence scores.

### 6.7 History is append-only at the audit boundary

The current graph may be updated, but every material mutation emits an immutable audit event. Decision revisions must remain queryable.

### 6.8 Context should be progressively disclosed

An agent begins with the destination, readiness failures, current frontier, current decisions, and active findings. Full node bodies and provenance are fetched only when needed.

## 7. Users and primary use cases

### 7.1 Primary users

- A developer using one coding agent across many sessions.
- Several local agents investigating independent branches of the same technical decision.
- A technical lead collaborating with an agent on architecture.
- A researcher moving from a vague destination to a defensible specification.

### 7.2 Primary workflows

1. **Chart an effort:** define a destination, exit criteria, visible questions, initial assumptions, and fog.
2. **Work the frontier:** claim one ready investigation, resolve it, add evidence, and expose newly specifiable questions.
3. **Make a decision:** record alternatives, selected choice, rationale, confidence, reversibility, and supporting evidence.
4. **Challenge a premise:** attach contradictory evidence, explicitly adjudicate it, and review affected decisions.
5. **Reopen a decision:** preserve the earlier revision, state why it was reopened, and resolve it again.
6. **Reconcile research:** surface apparently incompatible claims and create a focused question or experiment.
7. **Check readiness:** run deterministic convergence and lint checks.
8. **Handoff:** render the converged graph into an implementation-spec input or decision record.

## 8. Domain model

### 8.1 Workspace

A workspace is the stable container associated with a project or repository.

Required fields:

- `id`: ULID.
- `name`: human-readable name.
- `root_uri`: canonical local project URI.
- `created_at`, `updated_at`.
- `schema_version`.

### 8.2 Effort

An effort is one bounded journey from uncertainty to a destination.

Required fields:

- `id`: ULID.
- `workspace_id`.
- `slug`: unique within the workspace.
- `title`.
- `destination`: one or two precise paragraphs.
- `scope_notes`.
- `status`: `active | ready | completed | abandoned`.
- `version`: monotonically increasing optimistic-concurrency value.
- timestamps.

An effort owns nodes, edges, fog patches, findings, claims, exit criteria, and audit events.

### 8.3 Node kinds

All graph nodes share an identity, title, summary, body, lifecycle, validity, metadata, and revision history. Kind-specific payload is stored as validated JSON.

| Kind | Purpose | Important fields |
|---|---|---|
| `destination` | Graph anchor corresponding to the effort destination | exit criteria references |
| `question` | A precise question that research or human discussion can answer | answer, answer type |
| `decision` | A choice among explicit alternatives | prompt, alternatives, selected option, rationale |
| `assumption` | A proposition temporarily treated as true | statement, confidence, validation method |
| `evidence` | A claim grounded in one or more sources | claim, source refs, strength |
| `experiment` | A reproducible action designed to produce evidence | hypothesis, procedure, success criteria, result |
| `observation` | A directly observed fact that may not be a controlled experiment | statement, context, source refs |
| `constraint` | A boundary the solution must respect | statement, origin, hard/soft |
| `action` | Manual prerequisite work required to unblock inquiry | completion criteria |

`action` is intentionally narrow. Implementation deliverables do not belong in Threadmark merely because they are actionable.

### 8.4 Generic lifecycle

The persisted lifecycle enum is:

`draft | open | in_progress | resolved | out_of_scope | archived`

Kind-specific validation determines which states are legal. For example, an accepted assumption is represented by `resolved` plus validity `current`; an experiment may be `resolved` with a result or `archived` after cancellation.

### 8.5 Validity

Validity is separate from lifecycle:

`current | challenged | undermined | review_required | invalid | superseded | stale`

This separation allows a resolved decision to remain resolved historically while being marked `undermined` because one of its assumptions is no longer valid.

### 8.6 Confidence, reversibility, and risk metadata

Confidence:

`tentative | supported | strong`

Reversibility:

`easy | moderate | expensive`

Impact, uncertainty, and cost of being wrong:

`low | medium | high | critical` for impact and cost; `low | medium | high` for uncertainty.

Every confidence value must include `confidence_reason`. Unknown risk metadata must remain unknown in storage; ranking treats unknown uncertainty conservatively as high and other unknown dimensions as medium.

### 8.7 Decision payload

A decision must contain:

```json
{
  "prompt": "What should the cache granularity be?",
  "alternatives": [
    {
      "id": "repository",
      "label": "Repository-level",
      "status": "rejected",
      "reason": "Invalidation blast radius is too large"
    },
    {
      "id": "module",
      "label": "Module-level",
      "status": "selected",
      "reason": "Best measured balance of reuse and bookkeeping"
    },
    {
      "id": "symbol",
      "label": "Symbol-level",
      "status": "rejected",
      "reason": "Bookkeeping cost is not justified by current workloads"
    }
  ],
  "selected_option": "module",
  "rationale": "...",
  "confidence": "supported",
  "confidence_reason": "Benchmarked locally; production workload not yet measured",
  "reversibility": "moderate"
}
```

A resolved decision must select exactly one alternative. Alternative IDs are stable within the decision. Reopening appends a new node revision; it must not overwrite earlier alternatives or rationale in history.

### 8.8 Provenance

Evidence and observations may reference one or more sources.

Source fields:

- `id`: ULID.
- `kind`: `url | file | git_commit | pull_request | issue | benchmark | command_output | conversation | person | other`.
- `uri`: canonical identifier where possible.
- `title`.
- `retrieved_at` or `observed_at`.
- `content_hash`: optional SHA-256 of the referenced artifact or captured excerpt.
- `excerpt`: optional, length-limited text.
- `metadata_json`.
- `trust`: `unreviewed | reviewed | authoritative`.

Threadmark records provenance; it does not claim that a source is correct. Trust is assigned explicitly.

### 8.9 Fog patches

Fog is not represented as a normal graph node because it is, by definition, not yet precise enough to have graph semantics.

A fog patch contains:

- `id`, `effort_id`.
- `title`.
- `description`.
- optional `anchor_node_id`.
- `status`: `active | graduated | out_of_scope`.
- optional `graduated_to` node IDs.
- timestamps.

Convergence requires no active in-scope fog patches unless an exit criterion explicitly allows them.

## 9. Edge model

Every edge has `id`, `effort_id`, `source_node_id`, `type`, `target_node_id`, optional rationale, creator, and timestamps.

The direction is always read as an English sentence: **source relation target**.

| Edge | Semantics | Hard blocker? | Invalidation role |
|---|---|---:|---|
| `requires` | A requires B before A can be meaningfully resolved | Yes | Invalid B puts resolved A under review |
| `informs` | A provides useful information to B | No | Stale A may lower confidence or create a finding |
| `supports` | A is explicit support for B | No | Withdrawn/invalid A triggers support re-evaluation |
| `contradicts` | A and B make apparently incompatible claims | No | Creates an unresolved contradiction finding |
| `assumes` | A relies on assumption B | No at scheduling time by default | Invalid B directly undermines A |
| `produces` | Experiment/action A produced evidence/observation B | No | Establishes provenance |
| `resolves` | Decision/evidence/observation A resolves question B | Yes for completing B | Resolution becomes invalid if A is invalid |
| `supersedes` | New conclusion A intentionally replaces B | No | Marks B superseded |

### 9.1 Edge constraints

- Self-edges are forbidden.
- Duplicate edges with the same source, type, and target are forbidden.
- `requires` must form a DAG within an effort.
- `supersedes` must form an acyclic lineage.
- `contradicts` is semantically symmetric. Storage must canonicalize the pair so A/B and B/A cannot both exist.
- Cross-effort edges are not supported in v1. External context is referenced through provenance.
- `assumes` must target an assumption.
- `produces` must originate from an experiment or action and target evidence or observation.
- `resolves` must target a question.
- `supersedes` normally connects nodes of the same kind; exceptions require an explicit rationale.

## 10. State transitions

### 10.1 Investigation lifecycle

```mermaid
stateDiagram-v2
    [*] --> Draft
    Draft --> Open
    Open --> InProgress: claim
    InProgress --> Open: release or lease expires
    InProgress --> Resolved: record answer/result
    Open --> OutOfScope
    Resolved --> Open: reopen
    Resolved --> Archived
```

### 10.2 Validity lifecycle

```mermaid
stateDiagram-v2
    [*] --> Current
    Current --> Challenged: contradiction accepted
    Challenged --> Current: challenge rejected
    Challenged --> Invalid: invalidated
    Current --> Undermined: relied-on premise invalidated
    Undermined --> ReviewRequired: downstream propagation
    Undermined --> Current: reviewed and reaffirmed
    ReviewRequired --> Current: reviewed and reaffirmed
    Current --> Superseded: replacement accepted
    Current --> Stale: freshness policy fails
```

State changes must be explicit commands backed by transactions and audit events. Derived propagation may change validity automatically only according to the deterministic rules below.

## 11. Claims and concurrency

Claims prevent two local sessions from resolving the same frontier node.

Claim fields:

- `node_id`.
- `actor_id`: stable human or agent identity.
- `session_id`: unique invocation/session identity.
- `claimed_at`.
- `lease_expires_at`.
- `heartbeat_at`.

Rules:

1. Only `open` claimable nodes may be claimed.
2. Claim creation runs in an immediate SQLite transaction.
3. At most one unexpired claim may exist for a node.
4. Default lease: 30 minutes; configurable per workspace.
5. MCP clients should heartbeat every one-third of the lease duration.
6. An expired claim does not delete history. It becomes inactive and the node returns to the frontier.
7. A user may force-release a claim, with an audit reason.
8. Resolution requires ownership of the active claim unless `--force` is used by a human actor.
9. A batch mutation checks the effort's expected version to prevent stale writes.

Multi-machine leases and distributed consensus are out of scope for v1.

## 12. Frontier calculation

A node is on the frontier when all of the following are true:

1. Its kind is claimable: `question`, `decision`, `experiment`, or `action`.
2. Its lifecycle is `open`.
3. Its validity is not `invalid`, `superseded`, or `stale`.
4. It has no active unexpired claim.
5. Every target of its outgoing `requires` edge is resolved and not invalid, undermined, review-required, superseded, or stale.
6. It is not explicitly out of scope.

The default ordering is risk-weighted and deterministic. Sort descending by this tuple:

1. cost of being wrong;
2. impact;
3. uncertainty;
4. transitive downstream fan-out over reverse `requires` edges;
5. age, oldest first;
6. ULID as a stable final tie-breaker.

This deliberately avoids presenting a synthetic numeric score. The CLI must support `--strategy=fifo` for compatibility with simpler workflows.

The frontier response must include a human-readable explanation of why each node ranked where it did.

## 13. Contradiction handling

Threadmark distinguishes **suspected contradiction**, **accepted contradiction**, and **adjudicated outcome**.

1. A human or agent proposes a contradiction between two nodes.
2. The runtime creates a finding with status `proposed` and stores the explanation and proposer.
3. A human or authorized agent accepts or rejects the finding.
4. Acceptance creates a `contradicts` edge and marks both current nodes `challenged` unless already in a stronger invalid state.
5. A focused question or experiment should normally be created to reconcile the claims.
6. Adjudication may reaffirm both claims with clarified scope, invalidate one claim, split a claim, or retain an unresolved contradiction.
7. Resolving an accepted contradiction requires terminal endpoint states; any endpoint reaffirmation and the finding transition must share one atomic batch.

The core never asks an LLM on its own. A host agent may use `threadmark contradiction candidates` or the MCP equivalent to fetch likely comparison sets, run its own analysis, and submit proposals.

Contradictions are scoped. Claims that differ because they describe different versions, workloads, or environments should be refined rather than marked contradictory.

## 14. Invalidation propagation

Invalidation is deterministic after an explicit state change.

### 14.1 Rules

1. If an assumption becomes `invalid`, every current node with `assumes -> assumption` becomes `undermined`.
2. If a node becomes `invalid`, `undermined`, `review_required`, `superseded`, or `stale`, every resolved node that `requires` it becomes `review_required`.
3. Rule 2 propagates transitively over `requires` edges.
4. If a node resolving a question becomes invalid, the question becomes `open` unless another current resolving node remains.
5. If supporting evidence becomes invalid, withdrawn, or stale, the target is not automatically undermined. Instead, recompute support sufficiency and create a `support_gap` finding when its declared confidence policy is no longer met.
6. A `contradicts` edge alone challenges its endpoints but does not invalidate either.
7. A `supersedes` edge marks its target `superseded`, then applies Rule 2 to nodes that require the superseded target.
8. Reaffirming a node does not automatically clear descendants. Each affected descendant must be reviewed, though a batch review operation may reaffirm several nodes with one recorded rationale.

### 14.2 Propagation output

Every invalidating operation must return a preview before commit or a complete result after commit:

```json
{
  "changed": [
    {"node": "A12", "from": "current", "to": "invalid"},
    {"node": "D4", "from": "current", "to": "undermined"},
    {"node": "D9", "from": "current", "to": "review_required"}
  ],
  "reopened_questions": ["Q7"],
  "findings_created": ["F19"]
}
```

The CLI should require confirmation for interactive humans when more than a configurable number of nodes would change. MCP clients use a dry-run token: preview returns an opaque mutation token bound to the current effort version, and commit must present that token.

## 15. Decision reopening and history

Reopening a decision must:

1. Append a revision containing the reason and triggering nodes.
2. Change lifecycle to `open` and validity to `review_required`.
3. Preserve the previously selected alternative and rationale in earlier revisions.
4. Put the decision on the frontier when its hard prerequisites are satisfied.
5. Mark `requires` dependents as `review_required`.

Resolving the reopened decision appends another revision. It may reaffirm the same option or select a different one. If the scope or identity of the decision has fundamentally changed, create a new decision with `supersedes` instead of revising the old one.

## 16. Convergence and readiness

An effort is never declared ready using an LLM-generated percentage. Readiness is a boolean result plus a list of passed and failed criteria.

### 16.1 Built-in exit criteria

- `no_open_required_nodes`
- `no_active_fog`
- `no_undermined_decisions`
- `no_review_required_decisions`
- `no_blocking_findings`
- `requires_confidence_for_reversibility`
- `node_resolved`
- `node_valid`
- `evidence_count_at_least`
- `source_trust_at_least`

Example policy:

```yaml
exit_criteria:
  - type: no_open_required_nodes
  - type: no_active_fog
  - type: no_blocking_findings
  - type: requires_confidence_for_reversibility
    expensive: supported
  - type: node_resolved
    node: performance-benchmark
```

Custom criteria in v1 are data-driven combinations of supported predicates, not executable plugins.

### 16.2 Status output

```text
Incremental indexing architecture
Readiness: NOT READY

Passed
  ✓ No cycles or dangling references
  ✓ All expensive decisions are at least supported

Blocking
  ✗ Cache granularity is undermined by invalid assumption A12
  ✗ Performance benchmark question remains open
  ✗ One active fog patch remains

Frontier
  1. Benchmark invalidation fan-out
  2. Reconsider cache granularity
```

## 17. Core workflows

### 17.1 Chart an effort

1. Create the effort and destination.
2. Record explicit in-scope and out-of-scope boundaries.
3. Add structured exit criteria.
4. Breadth-first identify precise questions, decisions, assumptions, constraints, and experiments currently visible.
5. Add `requires` edges only where work is genuinely impossible without the prerequisite.
6. Add non-blocking semantic edges separately.
7. Record imprecise future areas as fog patches.
8. Run lint and display the initial frontier.
9. Stop. Charting must not opportunistically resolve the first human decision.

### 17.2 Work a frontier node

1. Load low-resolution effort context.
2. Select or claim one frontier node.
3. Load that node, its prerequisites, dependents, assumptions, support, and relevant revisions.
4. Perform research, discussion, prototype, experiment, or prerequisite action.
5. Record explicit evidence, observations, sources, or alternatives.
6. Resolve the node with an answer/result and confidence reason.
7. Add newly visible precise nodes and graduate relevant fog patches.
8. Run propagation, contradiction checks, lint, and readiness.
9. Release the claim automatically as part of the resolution transaction.

Independent research questions may be worked in parallel. A single session should otherwise resolve one substantive frontier node to keep reviewable boundaries.

### 17.3 Make a decision

1. Ensure the decision prompt and alternatives are explicit.
2. Link evidence and assumptions.
3. Record selected and rejected alternatives with reasons.
4. Set confidence, confidence reason, reversibility, impact, and cost of being wrong.
5. Resolve the decision.
6. Recompute frontier and readiness.

### 17.4 Challenge an assumption

1. Add or identify the contradictory evidence.
2. Propose and accept a contradiction finding.
3. Mark the assumption challenged.
4. Create a reconciliation question/experiment if necessary.
5. Explicitly reaffirm, refine, retire, or invalidate the assumption.
6. Preview and commit invalidation propagation.

### 17.5 Handoff

When all required exit criteria pass, `threadmark handoff` renders:

- destination and scope;
- constraints;
- accepted decisions and alternatives;
- active assumptions and their confidence;
- supporting evidence and provenance;
- experiments and results;
- known limitations and out-of-scope areas;
- residual non-blocking uncertainty;
- a decision-history appendix.

The handoff is an input to a specification or execution planner. It is not itself an execution DAG.

## 18. Architecture

### 18.1 Components

```mermaid
flowchart TD
    CLI["CLI"] --> Core["Domain core"]
    MCP["MCP server"] --> Core
    Core --> Store["SQLite store"]
    Core --> Export["Import/export"]
    Skill["Agent skill"] --> CLI
    Skill --> MCP
```

The CLI and MCP server must be thin adapters around the same application service layer. Business rules must not live in command handlers or MCP tool implementations.

### 18.2 Rust workspace

Recommended structure:

```text
threadmark/
  Cargo.toml
  crates/
    threadmark-domain/       # entities, enums, validation, graph algorithms
    threadmark-store/        # SQLite repositories and migrations
    threadmark-application/  # use cases, transactions, DTOs
    threadmark-cli/          # human and JSON CLI
    threadmark-mcp/          # stdio MCP server
    threadmark-export/       # Markdown/JSON export and import
  skills/
    threadmark/
      SKILL.md
  docs/
    architecture.md
    graph-semantics.md
```

Use stable Rust. Suggested libraries are `clap`, `serde`, `serde_json`, `schemars`, `sqlx` with SQLite, `tokio`, `ulid`, `time`, `thiserror`, and `tracing`. Exact versions should be selected and locked when implementation begins. The MCP adapter should be isolated behind an internal transport trait so a change in SDK does not affect domain code.

### 18.3 No daemon in v1

CLI commands open the database, execute a bounded transaction, and exit. The MCP server is a long-running stdio process with a small SQLite connection pool. A background daemon is not justified until remote synchronization, hosted scheduling, or automatic monitoring is implemented.

## 19. Workspace discovery and persistence

### 19.1 Project marker

`threadmark init` creates:

```text
.threadmark/
  workspace.toml
  exports/
```

`workspace.toml` contains a stable workspace ULID, display name, schema version, and non-secret configuration. It is safe to commit.

### 19.2 Database location

Default behavior:

- In a Git repository, store the database under the repository's Git common directory so linked worktrees share one local state: `<git-common-dir>/threadmark/state.sqlite3`.
- Outside Git, store it at `.threadmark/state.sqlite3` and add the database plus WAL/SHM files to `.gitignore`.
- Allow `THREADMARK_DATABASE_URL` and config overrides for tests and advanced setups.

The database is local runtime state, not a Git synchronization format.

### 19.3 Portable export

`threadmark export` writes a deterministic, reviewable package under `.threadmark/exports/<effort-slug>/`:

- `effort.yaml`
- `nodes/<id>.md` with YAML front matter
- `edges.yaml`
- `sources.yaml`
- `findings.yaml`
- `events.jsonl` optionally
- `handoff.md` when requested

Exports sort by stable ID and normalize timestamps/formatting. Secrets are never exported. `threadmark import` validates the full package before a transaction changes local state.

Automatic multi-writer Git merging is out of scope. The export format is a portability and review surface, not the live database.

## 20. SQLite schema

The implementation may adjust column types, but it must preserve the following logical model.

### 20.1 Tables

```sql
workspaces(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  root_uri TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

efforts(
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  destination TEXT NOT NULL,
  scope_notes TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(workspace_id, slug)
);

nodes(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  lifecycle TEXT NOT NULL,
  validity TEXT NOT NULL,
  confidence TEXT,
  confidence_reason TEXT,
  reversibility TEXT,
  impact TEXT,
  uncertainty TEXT,
  cost_of_wrong TEXT,
  current_revision INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(effort_id, id)
);

node_revisions(
  node_id TEXT NOT NULL REFERENCES nodes(id),
  revision INTEGER NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL,
  reason TEXT,
  actor_id TEXT NOT NULL,
  session_id TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(node_id, revision)
);

edges(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  source_node_id TEXT NOT NULL REFERENCES nodes(id),
  type TEXT NOT NULL,
  target_node_id TEXT NOT NULL REFERENCES nodes(id),
  rationale TEXT,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(source_node_id, type, target_node_id)
);

sources(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  kind TEXT NOT NULL,
  uri TEXT,
  title TEXT NOT NULL,
  retrieved_at TEXT,
  observed_at TEXT,
  content_hash TEXT,
  excerpt TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  trust TEXT NOT NULL,
  created_at TEXT NOT NULL
);

node_sources(
  node_id TEXT NOT NULL REFERENCES nodes(id),
  source_id TEXT NOT NULL REFERENCES sources(id),
  relationship TEXT NOT NULL,
  PRIMARY KEY(node_id, source_id, relationship)
);

fog_patches(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  anchor_node_id TEXT REFERENCES nodes(id),
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

fog_graduations(
  fog_id TEXT NOT NULL REFERENCES fog_patches(id),
  node_id TEXT NOT NULL REFERENCES nodes(id),
  PRIMARY KEY(fog_id, node_id)
);

claims(
  id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL REFERENCES nodes(id),
  actor_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  claimed_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  lease_expires_at TEXT NOT NULL,
  released_at TEXT,
  release_reason TEXT
);

exit_criteria(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  type TEXT NOT NULL,
  config_json TEXT NOT NULL,
  required INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

findings(
  id TEXT PRIMARY KEY,
  effort_id TEXT NOT NULL REFERENCES efforts(id),
  type TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT NOT NULL,
  related_nodes_json TEXT NOT NULL,
  proposed_by TEXT,
  adjudication TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

events(
  id TEXT PRIMARY KEY,
  effort_id TEXT REFERENCES efforts(id),
  actor_id TEXT NOT NULL,
  session_id TEXT,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  before_json TEXT,
  after_json TEXT,
  reason TEXT,
  occurred_at TEXT NOT NULL
);
```

### 20.2 SQLite configuration

On every connection:

- `PRAGMA foreign_keys = ON`.
- WAL journal mode.
- A configurable busy timeout, default five seconds.
- `synchronous = NORMAL` by default, configurable for stricter durability.

Migrations are forward-only and transactional where SQLite permits. Every supported binary must refuse to open a database with a newer unsupported schema.

## 21. Application service operations

The application layer exposes transport-neutral use cases:

- `CreateWorkspace`
- `CreateEffort`
- `UpdateEffort`
- `AddNode`
- `ReviseNode`
- `AddEdge`
- `RemoveEdge`
- `AddSource`
- `AttachSource`
- `AddFogPatch`
- `GraduateFogPatch`
- `ClaimNode`
- `HeartbeatClaim`
- `ReleaseClaim`
- `ResolveNode`
- `ReopenNode`
- `ProposeContradiction`
- `AdjudicateFinding`
- `PreviewInvalidation`
- `CommitInvalidation`
- `ReviewAffectedNodes`
- `GetFrontier`
- `GetEffortContext`
- `ExplainNode`
- `EvaluateReadiness`
- `LintEffort`
- `ExportEffort`
- `ImportEffort`
- `RenderHandoff`

All mutation DTOs include `actor_id`; multi-step agent mutations also include `session_id` and `expected_effort_version`.

## 22. CLI specification

### 22.1 Conventions

- Human-readable output is the default.
- Every read command supports `--json`.
- Every mutation supports `--dry-run` where propagation or batch effects are possible.
- IDs may be full ULIDs or unambiguous prefixes.
- Titles are shown alongside IDs in all human output.
- Non-interactive mode never prompts; ambiguity is an error.
- Exit code `0` means success, `1` user/data error, `2` lint/readiness failure when used as a gate, and `3` storage/internal error.

### 22.2 Commands

```text
threadmark init
threadmark effort create|list|show|update|complete|reopen|abandon
threadmark context [effort]
threadmark status [effort]
threadmark frontier [effort] [--strategy risk|fifo]

threadmark node add|show|edit|list|archive
threadmark question add|answer|reopen
threadmark decision add|resolve|reopen|supersede
threadmark assumption add|challenge|reaffirm|invalidate|retire
threadmark evidence add|withdraw|mark-stale
threadmark experiment add|start|resolve|fail
threadmark observation add
threadmark constraint add|relax|retire
threadmark action add|complete

threadmark edge add|remove|list
threadmark source add|attach|show
threadmark fog add|list|graduate|out-of-scope

threadmark claim next|node|heartbeat|release|list
threadmark finding list|show|accept|reject|resolve
threadmark contradiction propose|candidates
threadmark invalidate preview|commit
threadmark review affected|reaffirm|revise

threadmark why <node>
threadmark history [node]
threadmark lint [effort]
threadmark readiness [effort]
threadmark handoff [effort] --output <path>
threadmark export [effort] --output <dir>
threadmark import <dir>
threadmark mcp serve
```

### 22.3 `why` command

`threadmark why <decision>` is a central user-facing feature. It must show:

- current selected alternative and rationale;
- confidence and reversibility;
- supporting and informing nodes;
- assumptions;
- rejected alternatives;
- relevant provenance;
- prior revisions;
- current validity and any invalidation chain.

It must use graph traversal, not generated prose. Human output may summarize stored prose, while `--json` returns the exact structured paths.

## 23. MCP server specification

The MCP server runs over stdio in v1. Tool names rely on the MCP server namespace
and do not repeat the `threadmark_` prefix.

### 23.1 Read tools

- `list_efforts`
- `get_context`
- `get_snapshot`
- `get_frontier`
- `get_node`
- `explain_node`
- `get_readiness`
- `lint`
- `get_history`
- `render_handoff`

### 23.2 Claim tools

- `claim_next`
- `claim_node`
- `heartbeat_claim`
- `release_claim`

### 23.3 Mutation tools

- `apply_batch`
- `create_effort`
- `reopen_effort`
- `add_fog`
- `resolve_node`
- `reopen_node`
- `propose_contradiction`
- `adjudicate_finding`
- `preview_invalidation`
- `commit_invalidation`
- `graduate_fog`

### 23.4 Batch mutation

Agents should normally submit one atomic change set after completing an investigation:

```json
{
  "effort": "...",
  "expected_effort_version": 17,
  "actor_id": "codex",
  "session_id": "session-...",
  "operations": [
    {"op": "add_source", "temp_id": "s1", "kind": "url", "title": "Benchmark", "uri": "https://example.com"},
    {"op": "add_node", "temp_id": "e1", "value": {"kind": "evidence", "title": "Benchmark result", "summary": "...", "body": "...", "payload": {}, "lifecycle": "resolved"}},
    {"op": "attach_source", "node": "e1", "source": "s1", "relationship": "supports"},
    {"op": "resolve_node", "node": "Q1", "body": "...", "reason": "Investigation complete"},
    {"op": "add_edge", "source": "e1", "type": "resolves", "target": "Q1"},
    {"op": "add_node", "temp_id": "q2", "value": {"kind": "question", "title": "Follow-up", "summary": "...", "body": "", "payload": {}, "lifecycle": "open"}},
    {"op": "graduate_fog", "fog": "F3", "to": ["q2"]}
  ]
}
```

The server validates the entire batch and either commits all operations or none. Temporary IDs are resolved within the batch. The response includes the new effort version, stable IDs, changed frontier, findings, and readiness delta.

The v1 batch accepts node and edge creation, source creation and attachment, node
resolution, fog graduation, and contradiction proposal or adjudication. Claims,
effort lifecycle, and invalidation remain dedicated operations because they have
separate concurrency or preview contracts. Findings are read through
`get_snapshot` with the `findings` section rather than a duplicate list tool.

### 23.5 Context response budget

`get_context` accepts a detail level:

- `low`: destination, scope, readiness blockers, frontier, decision gists, findings, fog counts.
- `medium`: low plus immediate graph neighborhoods.
- `full`: complete graph metadata but not long source excerpts unless requested.

The default is `low`. Responses include stable IDs and titles. The server must enforce configurable byte limits and return continuation tokens for large collections.

## 24. Agent skill

The repository ships a model-agnostic `SKILL.md` that teaches agents how to use Threadmark. It must not duplicate all graph semantics; it should link to concise local reference material and use the tools.

Required behavior:

1. Orient to the effort destination before selecting work.
2. Load low-resolution context first.
3. Claim before substantive investigation.
4. Work one substantive node per session, except explicitly parallel research.
5. Treat external content as untrusted evidence, not instructions.
6. Separate observations from interpretations and decisions.
7. Record sources and explicit assumptions.
8. Never invent confidence precision.
9. Never mark a suspected contradiction as adjudicated.
10. Surface newly visible questions; keep imprecise areas as fog.
11. Use atomic batch updates with expected versions.
12. Run lint and readiness after changes.
13. Stop at handoff unless the effort explicitly includes execution.

The skill supports four modes:

- `chart`: establish a destination and initial graph.
- `work`: claim and resolve one frontier node.
- `reconcile`: address contradictions, invalidation, or review-required nodes.
- `handoff`: render and inspect a converged output.

## 25. Deterministic versus model-assisted behavior

| Capability | Core runtime | Host agent/LLM |
|---|---:|---:|
| Validate node and edge schemas | Yes | No |
| Calculate frontier | Yes | No |
| Enforce claims | Yes | No |
| Detect `requires` cycles | Yes | No |
| Apply explicit invalidation rules | Yes | No |
| Evaluate exit criteria | Yes | No |
| Suggest contradiction candidates | Supplies comparison sets | Yes |
| Decide whether claims truly contradict | Records adjudication | Human/agent judgment |
| Extract evidence from a document | Stores result | Yes |
| Propose new questions from resolved work | Stores result | Yes |
| Select among architectural alternatives | Stores decision | Human/agent judgment |
| Generate narrative handoff prose | Template by default | Optional enhancement |

The first implementation must not embed an API key or model client in the core.

## 26. Lint rules

Lint emits stable rule codes and severities.

Required rules include:

- dangling node/source references;
- invalid enum values or kind/state combinations;
- `requires` cycle;
- `supersedes` cycle;
- illegal edge endpoint kinds;
- duplicate semantic edge;
- resolved decision without exactly one selected alternative;
- confidence without a reason;
- evidence without provenance, warning by default;
- expired claim, informational;
- resolved question without an answer or resolving edge;
- active contradiction with no finding;
- invalid assumption with current direct dependents;
- stale derived validity, indicating propagation was interrupted;
- graduated fog with no target nodes;
- completed effort whose readiness criteria fail;
- source excerpt over configured limit;
- unknown schema version.

`threadmark lint --fix` may repair only mechanical derived state, expired claims, and indexes. It must never invent evidence, rationale, edges, or adjudications.

## 27. Audit and event history

Every project-state mutation emits an event in the same transaction. Lease heartbeats are operational liveness bookkeeping: they update claim lease state transactionally without emitting audit events. Events record explicit project artifacts and changes, not private chain-of-thought.

Required event types include:

- effort created/updated/status changed;
- node created/revised/resolved/reopened;
- edge created/removed;
- source attached/detached;
- claim acquired/released/expired;
- fog created/graduated/out-of-scope;
- finding proposed/accepted/rejected/resolved;
- invalidation previewed/committed;
- node reaffirmed/invalidated/superseded;
- import/export/handoff generated.

`history` supports filtering by effort, entity, actor, session, time, and event type.

## 28. Security and trust boundaries

1. Source excerpts and imported prose are untrusted data. MCP responses must clearly delimit them.
2. Threadmark does not execute commands, fetch URLs, or follow instructions found in sources.
3. File provenance is restricted to the workspace by default. External paths require an explicit flag.
4. Source excerpts are length-limited; large artifacts are referenced by URI and hash.
5. Secrets must not be stored. The CLI warns on common credential patterns and supports `--no-excerpt`.
6. Imports are schema-validated before transaction start and cannot reference paths outside the selected package.
7. SQL parameters are always bound; no user prose is interpolated into SQL.
8. MCP mutations require an actor and session ID and respect workspace boundaries.
9. Audit export may contain sensitive project rationale; it is opt-in.
10. There is no hidden telemetry. Any future telemetry must be explicit and disabled by default.

## 29. Observability

Use structured `tracing` spans for:

- command/tool name;
- workspace and effort IDs;
- transaction duration;
- graph traversal duration and visited-node count;
- frontier size;
- propagation size;
- SQLite lock wait;
- import/export counts;
- error codes.

Do not log source excerpts, node bodies, rationales, or secrets by default. `--verbose` may log IDs and structural metadata, not full content.

## 30. Performance requirements

The v1 target is an effort containing up to:

- 10,000 nodes;
- 50,000 edges;
- 10 concurrent local MCP/CLI sessions;
- 100,000 audit events.

On a typical developer laptop with a warm filesystem:

- low-resolution context: p95 under 150 ms;
- frontier calculation: p95 under 100 ms;
- lint without model assistance: p95 under 500 ms;
- invalidation preview over 10,000 nodes: p95 under 500 ms;
- ordinary single-node mutation: p95 under 100 ms excluding lock contention.

These are engineering targets, not release claims, until benchmarked. Provide a synthetic graph generator and Criterion benchmarks for traversal and propagation. Add SQLite indexes for effort/kind/state, edge source/type, edge target/type, active claims, findings, and event filters.

## 31. Error model

Application errors use stable codes:

- `NOT_INITIALIZED`
- `EFFORT_NOT_FOUND`
- `NODE_NOT_FOUND`
- `AMBIGUOUS_ID`
- `INVALID_STATE_TRANSITION`
- `INVALID_EDGE`
- `CYCLE_DETECTED`
- `CLAIM_CONFLICT`
- `CLAIM_EXPIRED`
- `VERSION_CONFLICT`
- `READINESS_FAILED`
- `LINT_FAILED`
- `IMPORT_INVALID`
- `SCHEMA_TOO_NEW`
- `STORAGE_BUSY`
- `INTERNAL`

Errors include a concise message, structured detail, and a safe remediation where one exists. MCP errors must preserve these codes.

## 32. Testing strategy

### 32.1 Unit tests

- Every legal and illegal state transition.
- Edge endpoint validation.
- Cycle detection.
- Frontier eligibility and ordering.
- Confidence/reversibility exit policies.
- Invalidation propagation.
- Support-gap detection.
- Contradiction canonicalization.
- Decision revision history.

### 32.2 Property-based tests

- Propagation terminates for arbitrary valid DAGs.
- Adding a `requires` edge that creates a cycle is always rejected.
- Frontier never contains claimed, blocked, invalid, or out-of-scope nodes.
- Preview followed by commit at the same version produces exactly the previewed changes.
- Export/import round trips preserve semantic state.
- Reordering independent mutations does not change derived graph state.

### 32.3 Store integration tests

- Migration from every released schema fixture.
- Concurrent claim races: exactly one winner.
- Busy-timeout behavior.
- Transaction rollback for a failed batch.
- MCP server and CLI observe the same committed state.
- Git linked-worktree database discovery.

### 32.4 Golden tests

- Human CLI output.
- JSON schemas and responses.
- Markdown/YAML export.
- Handoff rendering.
- `why` explanation paths.

### 32.5 End-to-end acceptance scenario

Build a fixture effort for cache architecture:

1. Create destination and exit criteria.
2. Add storage research, workload observation, and cache decision.
3. Run two independent claims concurrently.
4. Resolve both with sourced evidence.
5. Select module-level caching and reject alternatives.
6. Add an assumption that module boundaries are stable.
7. Add a benchmark contradicting that assumption.
8. Accept the contradiction, invalidate the assumption, and verify the cache decision becomes undermined.
9. Verify its dependent invalidation strategy becomes review-required.
10. Reopen and revise the cache decision.
11. Resolve all findings and fog.
12. Verify readiness passes and the handoff explains both decision revisions.

## 33. Implementation phases

### Phase 0: Repository and contracts

Deliver:

- Rust workspace and CI.
- Domain enums and JSON Schemas.
- ADRs for deterministic core, SQLite, validity/lifecycle separation, and local-first scope.
- A fixture graph and golden expected outputs.

Exit condition: schemas and state-transition tests are agreed before persistence or UI work expands.

### Phase 1: Core graph and SQLite

Deliver:

- migrations and repositories;
- workspace/effort/node/edge/source/fog CRUD;
- validation and audit events;
- frontier calculation;
- lint;
- readiness predicates;
- human and JSON CLI for the core read/write operations.

Exit condition: the cache-architecture fixture can be created entirely through the CLI and passes deterministic golden tests.

### Phase 2: Claims, history, and invalidation

Deliver:

- expiring transactional claims;
- decision revisions and reopen flow;
- findings and contradiction adjudication;
- invalidation preview/commit;
- support-gap findings;
- `why` and history traversal.

Exit condition: the complete end-to-end acceptance scenario passes, including concurrent claim races.

### Phase 3: MCP and agent skill

Deliver:

- stdio MCP server;
- read, claim, mutation, and invalidation tools;
- atomic batch change sets;
- low/medium/full context budgets;
- model-agnostic Threadmark skill;
- example configuration for Codex and Claude Code.

Exit condition: two agent sessions can independently claim and resolve separate research questions without direct database or file editing.

### Phase 4: Portability and handoff

Deliver:

- deterministic export/import;
- Markdown handoff renderer;
- redaction and source-excerpt controls;
- round-trip and golden tests.

Exit condition: an effort exported on one machine can be imported into an empty workspace with identical semantic state.

### Phase 5: Optional viewer and model-assisted proposals

Deliver only after core stabilization:

- read-only local graph viewer;
- contradiction comparison-set generation;
- host-agent recipes for claim extraction and reconciliation;
- visualization of invalidation chains and decision lineage.

The viewer must consume public application APIs, not query SQLite directly.

## 34. Definition of done for v1

Threadmark v1 is complete when:

1. All Phase 0–4 exit conditions pass.
2. The CLI and MCP server use the same application services.
3. A 10,000-node synthetic fixture meets the performance targets or documented deviations are accepted.
4. Concurrent claims have a deterministic one-winner test.
5. Invalidation preview and commit are version-bound and auditable.
6. Every project-state mutation has an event and no event contains hidden model reasoning.
7. Export/import round trips are semantically identical.
8. The shipped skill completes the acceptance scenario with at least two supported agent hosts.
9. Documentation explains edge direction, lifecycle versus validity, and the deterministic/model boundary.
10. No model provider, hosted service, or daemon is required.

## 35. Deferred decisions

These questions should be resolved during Phase 0 or recorded as a first Threadmark effort rather than guessed during implementation:

1. Whether `sqlx` or `rusqlite` offers the better simplicity/testability tradeoff for the final MCP concurrency model.
2. The exact Rust MCP SDK and how its JSON Schema generation is isolated.
3. Whether external IDs should expose full ULIDs, generated short aliases, or both.
4. Whether source trust is assigned per source or per source-to-claim relationship.
5. Whether an accepted contradiction should challenge both endpoints automatically or only the challenged target selected during adjudication.
6. Whether portable exports include the full event log by default or only by explicit flag.
7. Whether the handoff renderer is purely templated in v1 or permits an optional host-agent narrative pass.
8. Final public name, package availability, and trademark/domain checks.

None of these should block implementing the domain model, graph algorithms, and fixtures.

## 36. Instructions for the implementing agent

The implementing agent should treat this document as the product contract and proceed in phases.

For each phase:

1. Inspect the existing repository and its contribution instructions before changing files.
2. Create or update the relevant ADR before introducing a material architectural deviation.
3. Implement the smallest vertical slice that proves the phase's exit condition.
4. Keep graph semantics in `threadmark-domain` and transactional orchestration in `threadmark-application`.
5. Do not let CLI, MCP, or SQLite concerns leak into domain algorithms.
6. Write tests before or alongside every state transition and propagation rule.
7. Use the cache-architecture fixture as the continuous end-to-end example.
8. Do not add a model provider, daemon, web UI, vector database, remote synchronization, or task executor unless the specification is explicitly revised.
9. When this specification is ambiguous, preserve explicit history, deterministic behavior, and human review over convenience.
10. At the end of each phase, report delivered behavior, test evidence, performance measurements, deviations, and remaining deferred decisions.

## 37. Source lineage

Threadmark's conceptual starting points are:

- [Matt Pocock's Wayfinder skill](https://github.com/mattpocock/skills/blob/main/skills/engineering/wayfinder/SKILL.md), especially destination-first planning, the actionable frontier, one-decision sessions, and fog of war.
- [Wayfinder Maps](https://github.com/rengwu/wayfinder-maps), especially machine-checkable local state, claims, linting, frontier calculation, and undermined decisions.
- [RefoundAI Ralph](https://github.com/RefoundAI/ralph), as a useful contrast: an execution DAG with atomic claims and verification rather than an epistemic decision graph.

Threadmark should remain compatible with the spirit of Wayfinder while owning a distinct contract: it is a durable reasoning runtime, not an issue-tracker convention and not an execution scheduler.
