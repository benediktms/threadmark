# ADR 0002: Use SQLite as the local source of truth

## Status

Accepted

## Decision

Use SQLite with foreign keys, WAL, a five-second busy timeout, and transactional
mutations. In Git repositories, state lives under the Git common directory so
linked worktrees share claims and graph state. Deterministic Markdown/YAML
exports provide portability; the live database is not a Git merge format.

## Consequences

- Local sessions get atomic claims and optimistic concurrency.
- No daemon is required for CLI use.
- Multi-machine synchronization is deferred.
- Export/import must be stable and fully validated.
