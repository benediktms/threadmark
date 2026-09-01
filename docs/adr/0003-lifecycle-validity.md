# ADR 0003: Separate lifecycle from validity

## Status

Accepted

## Decision

A node's workflow lifecycle (`open`, `in_progress`, `resolved`, and so on) is
stored independently from whether its conclusion remains usable (`current`,
`undermined`, `review_required`, and so on).

## Consequences

A resolved decision remains part of history when a premise fails. Threadmark can
mark it undermined without incorrectly claiming that the original work was never
completed or that the conclusion is necessarily false.
