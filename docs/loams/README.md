# Ostrium Labs fork of Apache Iggy

This repository, [ostrium-labs/iggy](https://github.com/ostrium-labs/iggy), is
Ostrium Labs' fork of [apache/iggy](https://github.com/apache/iggy). It exists
to build Kafka wire compatibility in Iggy's Kafka gateway (`gateways/kafka/`)
for [Loams](https://github.com/ostrium-labs/loams). It is not an Apache
Software Foundation release, and the ASF does not endorse it. The code stays
under the Apache License 2.0, with the upstream `LICENSE`, `NOTICE` and ASF
headers.

## Branches

| Branch | What it is |
| --- | --- |
| `master` | An untouched mirror of `apache/iggy` `master`. Only fast-forwarded from upstream, never committed to |
| `loams` | The working branch and the fork's default branch. Every fork PR targets it |

## Syncing with upstream

```bash
git fetch upstream
git checkout master && git merge --ff-only upstream/master && git push origin master
git checkout loams && git rebase master        # or merge, if a rebase is too costly
git push --force-with-lease origin loams
./scripts/loams/disable-upstream-workflows.sh  # a workflow upstream added starts active
```

Keep the fork rebase-friendly: small, focused commits, no reformatting of
upstream code, new code in new files where that is natural. When a maintainer
has an open upstream PR for an item, build on it (cherry-pick with
attribution) instead of rewriting it, and drop the fork's copy once upstream
merges its own.

## CI

Upstream's workflows are disabled in this fork (`gh workflow disable`), because
the organisation's GitHub Actions concurrency is shared with the Loams
monorepo. One lean workflow, `.github/workflows/loams-kafka-gateway.yml`,
builds `iggy-server`, lints the gateway crate and runs its test suite,
including the real-client end-to-end tests. Its summary check,
`Kafka gateway required`, is what fork PRs merge on.

## Upstream

Nothing is posted to apache/iggy (issues, comments or PRs) without the Loams
owner's consent. Upstream's contribution rules (`CONTRIBUTING.md`, `AGENTS.md`)
still apply to anything that goes upstream later: the issue must be assigned
first, and one PR per issue.

## Plan

[`kafka-wire-plan.md`](kafka-wire-plan.md) lists the Kafka parity tasks, their
order, tests and status.
