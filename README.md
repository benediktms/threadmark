# Threadmark

Threadmark is a local-first reasoning and decision runtime for humans and AI
agents working on uncertain, long-running projects.

Instead of turning uncertainty into a premature task list, Threadmark stores a
typed graph of questions, decisions, assumptions, evidence, experiments,
observations, constraints, and prerequisites. It deterministically calculates
the actionable frontier, preserves provenance and rejected alternatives, and
shows which decisions need review when their premises change.

## Status

Threadmark is an early implementation of the design in
[`threadmark-implementation-spec.md`](threadmark-implementation-spec.md). The
current release targets a single machine with concurrent local agent sessions.

## Design boundaries

- The core is deterministic and model-independent.
- LLMs may propose graph mutations; they do not calculate canonical state.
- Threadmark is a reasoning graph, not an implementation task scheduler.
- SQLite is the local source of truth; deterministic exports provide a portable
  and reviewable representation.
- No hidden chain-of-thought is stored—only explicit project artifacts and
  rationales.

## Build

```sh
cargo build --workspace
cargo test --workspace
```

Install both binaries:

```sh
cargo install --path crates/threadmark-cli
cargo install --path crates/threadmark-mcp
```

## Quick start

```sh
threadmark init --name my-project
threadmark effort create architecture \
  --title "Choose the target architecture" \
  --destination "Produce an implementation-ready architecture."

threadmark node add architecture question \
  --title "What consistency guarantees are required?" \
  --summary "Determine the required consistency model." \
  --impact high --uncertainty high --cost-of-wrong critical

threadmark frontier architecture
threadmark claim next architecture --actor codex --session session-1
threadmark status architecture
```

Use `--json` on read commands for automation. Start the MCP server with:

```sh
threadmark-mcp --workspace .
```

## Repository layout

- `threadmark-domain`: entities, validation, graph algorithms
- `threadmark-store`: SQLite persistence and migrations
- `threadmark-application`: transactional use cases
- `threadmark-cli`: human and JSON CLI
- `threadmark-mcp`: stdio MCP adapter
- `threadmark-export`: deterministic export and handoff rendering
- `skills/threadmark`: model-independent agent workflow

## License

MIT
