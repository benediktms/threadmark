# ADR 0001: Keep canonical reasoning state deterministic

## Status

Accepted

## Context

Threadmark is designed for agent-assisted work, but graph state must remain
reproducible and auditable across sessions and model providers.

## Decision

Frontier calculation, validation, claims, state transitions, invalidation
propagation, lint, and readiness are implemented as deterministic Rust code.
Models may propose explicit changes through the CLI or MCP API. A proposal does
not change state until it passes the same validation as a human-authored change.

Threadmark stores explicit answers, evidence, alternatives, and rationales. It
does not store private chain-of-thought.

## Consequences

- The runtime remains useful without a model provider.
- Results can be covered by unit and property tests.
- Model-assisted contradiction discovery remains advisory.
- Agent integrations require explicit mutation operations.
