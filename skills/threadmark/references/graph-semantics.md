# Threadmark graph semantics

Read every edge as **source relation target**.

| Edge | Meaning |
|---|---|
| `requires` | Source cannot be resolved until target is resolved and usable |
| `informs` | Source provides useful, non-blocking information to target |
| `supports` | Source explicitly supports target |
| `contradicts` | Source and target contain apparently incompatible claims |
| `assumes` | Source relies on target, which must be an assumption |
| `produces` | An experiment/action source produced evidence/observation target |
| `resolves` | A result source resolves a question target |
| `supersedes` | Source intentionally replaces target |

`requires` and `supersedes` must be acyclic. `contradicts` is symmetric. A
contradiction challenges claims but does not automatically invalidate either.

Lifecycle describes whether work happened:

`draft -> open -> in_progress -> resolved`

Resolved work can be reopened. Out-of-scope and archived nodes do not appear on
the frontier.

Validity describes whether a conclusion remains usable:

`current | challenged | undermined | review_required | invalid | superseded | stale`

Propagation after an explicit invalidation is deterministic:

1. Invalid assumptions undermine direct `assumes` dependents.
2. Invalid, undermined, review-required, superseded, or stale prerequisites put
   resolved `requires` dependents under review.
3. This review requirement propagates transitively over `requires`.
4. A question reopens if all nodes resolving it become unusable.
5. Lost supporting evidence creates a support gap; it does not automatically
   invalidate the supported conclusion.

Frontier nodes are open, claimable, usable, unclaimed, and have every
`requires` target resolved and usable. The default order prioritizes cost of
being wrong, impact, uncertainty, downstream fan-out, and age.

Readiness is a pass/fail result with explicit failed criteria. Never replace it
with a model-generated percentage.
