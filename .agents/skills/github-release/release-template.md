# Release notes template

GitHub title is always `vX.Y.Z` — set with `--title "vX.Y.Z"`. The body holds
the prose headline.

## Single-change release (most common)

```markdown
## <Headline>

<1-2 sentence lead — what the user sees now that they didn't before. No
implementation details.>

See [`docs/X.md`](https://github.com/subsquid/sqd-portal/blob/vNEW/docs/X.md) for the full lifecycle, configuration, and deployment notes. (#PR)

**Full Changelog**: https://github.com/subsquid/sqd-portal/compare/vPREV...vNEW
```

## Multi-change release

```markdown
## <Headline>

<1-2 sentence lead.>

- **Bold lede.** Short explanation of change 1.
- **Bold lede.** Short explanation of change 2.

See [`docs/X.md`](...) for full details. (#PR)

**Full Changelog**: ...
```

## Style rules

- **Headline is a concept, not a version.** "Graceful shutdown",
  "Larger query responses". No emoji. No leading verb.
- **Lead with user-visible impact**, not internal mechanism. "The portal
  can now handle larger responses from network workers" beats "Bumped
  `sqd-network-transport` to a.b.c".
- **No deployment / ops instructions.** No `terminationGracePeriodSeconds`,
  no `preStop`, no kubectl recipes. Those belong in `docs/X.md` or a
  runbook. If a release requires operator action, the doc link conveys
  that — the notes don't repeat the recipe.
- **No CI / internal changes.** Skip workflow tweaks, clippy fixes,
  refactors. Release notes are for behavior the user observes.
- **No specific tools, config keys, or benchmark numbers in the body.**
  Generalize. `nginx-ingress` → "upstream load balancers".
  `pre_drain_grace_period_sec=25` → "a configurable grace period". Point
  to the doc for the real values.
- **Doc + PR ref** as a single line at the end of the prose, before the
  compare link. Anchor on the tag (`/blob/vNEW/`) so the link doesn't
  rot when `master` evolves.
- **Compare link always last.** Resolve `vPREV` with
  `git merge-base vPREV vNEW` when the previous-tag-in-sort-order isn't
  the real parent — minor branches that branched off an earlier point
  break the naive sort.

## What to skip entirely

- Install / upgrade commands (`docker pull`, `kubectl apply`) — live in deploy docs.
- The full commit message — `git log` is for the engineer's-eye view; notes are for the user's.
- "Tests" section on patch releases unless the headline is about test coverage.
- Internal cluster names, account IDs, environment labels.
