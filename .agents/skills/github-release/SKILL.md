---
name: github-release
description: Cut a new GitHub release for sqd-portal — bump Cargo.toml version, tag, push, let CI build/publish the docker image, then create the GitHub release page with standardized notes. Use when the user asks to "release", "publish", "cut vX.Y.Z", or "ship".
metadata:
  internal: true
---

# sqd-portal release

End-to-end release procedure. Bumps `Cargo.toml`, tags `vX.Y.Z`, pushes, lets
`.github/workflows/docker.yml` build and publish the docker image, then
creates the GitHub release page with standardized notes.

## Preconditions

Confirm before starting:

- `git status` is clean on `master` (or the user has staged the version bump deliberately).
- The user named a target version, e.g. `0.10.1`. If not, ask.
- The current `version` in `Cargo.toml` is the previous release. Mismatched
  bumps that landed silently in earlier commits happen — verify before tagging.
- For RCs use a suffix: `v0.10.1-rc1`. The CI trigger is `tags: ['v**']` so
  both release and rc tags fire it.

## Steps

### 1. Bump version

Edit `Cargo.toml`:

```toml
version = "X.Y.Z"
```

Run `cargo build` once so `Cargo.lock` updates. Don't ship a bump without the
matching lockfile change — CI will surface it later otherwise.

### 2. Commit and tag

```sh
git add Cargo.toml Cargo.lock
git commit -m "Bump version"
git tag vX.Y.Z
git push origin master
git push origin vX.Y.Z
```

Both pushes are required — the docker workflow triggers on the tag push, not
the commit.

### 3. Watch the docker build

```sh
RUN_ID=$(gh run list --repo subsquid/sqd-portal --workflow=docker.yml --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo subsquid/sqd-portal --exit-status
```

The workflow delegates to `subsquid/github-workflows/.github/workflows/docker-on-tag.yml`
and publishes `subsquid/sqd-portal:vX.Y.Z` (+ `:latest`). Build time ~5–10 min
for multi-arch. If it fails, surface the log — common causes are dep
resolution timeouts and registry hiccups; don't retry blindly.

### 4. Create the GitHub release page

The docker workflow does **not** create the release page — do it explicitly.
Use [release-template.md](release-template.md) for the body.

```sh
gh release create vX.Y.Z --repo subsquid/sqd-portal --title "vX.Y.Z" --notes "$(cat <<'EOF'
## <Headline>

<lead, bullets, doc link, compare link — see release-template.md>
EOF
)"
```

For a tag that was pushed earlier without a release page:

```sh
gh release edit vX.Y.Z --repo subsquid/sqd-portal --title "vX.Y.Z" --notes "..."
```

### 5. Confirm

Print: `https://github.com/subsquid/sqd-portal/releases/tag/vX.Y.Z`

## Release notes format

See [release-template.md](release-template.md). Hard rules learned the hard way:

- **GitHub title = `vX.Y.Z`.** This repo's convention. The body's `## <Headline>`
  is the prose headline; the UI shows tag and title side-by-side.
- **No deployment / ops instructions.** Release notes describe *what changed in
  the software*, not how to roll it out. `terminationGracePeriodSeconds`,
  `preStop` hooks, kubelet config — all of that belongs in `docs/X.md` or a
  runbook. If you catch yourself writing "Action required for Kubernetes",
  move it out.
- **No CI / internal-only changes.** Skip `cargo test` workflow additions,
  clippy fixes, refactors that don't change observable behavior.
- **General > specific.** Don't name specific tools (`nginx-ingress`), config
  keys (`pre_drain_grace_period_sec`), or your own benchmark numbers in the
  body. Point to a doc for those details.
- **Doc + PR ref** as a one-liner at the end of the prose, before the compare
  link: `See [`docs/X.md`](url) for ... (#PR)`.
- **Compare link.** Always end with
  `**Full Changelog**: https://github.com/subsquid/sqd-portal/compare/vPREV...vNEW`.
  Resolve `vPREV` with `git merge-base vPREV vNEW` when branching is
  non-linear — the previous tag in `git tag --list` sorted order is not always
  the right base.

## Failure modes

- **Tag exists**: `git tag vX.Y.Z` fails. Either the user already tagged or a
  previous attempt didn't complete. Check `gh release view vX.Y.Z` and
  `gh run list --workflow=docker.yml`. For an RC that needs to move to a
  different commit, force-update: `git tag -f vX.Y.Z <newcommit>` +
  `git push -f origin vX.Y.Z`. Destructive — confirm with the user first.
- **Local docker build with `--platform linux/amd64` from a Mac silently
  produces a broken image.** The default Dockerfile uses
  `FROM --platform=$BUILDPLATFORM`, so on an arm64 Mac you get an arm64 ELF
  in an amd64 OCI manifest — `exec format error` on the target host. Never
  recommend the local-build-and-push path for production images. CI handles
  multi-arch correctly; route the user there instead.
- **CI build fails on tag push but a release page already exists.** The page
  is independent of the image. Delete or skip the page; fix the build; retag
  if needed.
