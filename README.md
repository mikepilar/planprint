# planprint

Query plan fingerprinting and redundancy detection for Snowflake.

planprint identifies structurally duplicate computation happening across
independent queries and pipelines — the kind of redundancy that accumulates
invisibly as teams grow and AI accelerates pipeline creation. Each individual
query looks fine. The cost accumulates across all of them.

## How it works

planprint runs in two passes:

**Sweep** — broad structural matching. Strips the SELECT list, all literal
values, aliases, schema prefixes, and filter predicates. Two queries that
join the same tables in the same topology produce the same sweep fingerprint
regardless of what they filter on or what columns they return.

**Refine** — computation sub-clustering. Within each sweep cluster, groups
queries further by aggregation pattern, window functions, and GROUP BY
structure. Queries that share join topology but compute different things need
different materialized views and land in separate sub-clusters.

Fingerprints are stored in a persistent table so comparisons run against
captured history rather than live query profiles, which expire after ~14 days
in Snowflake.

## What it catches

When 3 or more distinct queries share a sweep and refine fingerprint, planprint
flags the cluster for operator review. The operator examines the cluster and
decides whether to materialize the shared join structure as a view, then
rewrite the queries to filter against the view rather than re-scanning and
re-joining the underlying tables.

## Stack

- [sqlglot](https://github.com/tobymao/sqlglot) — SQL parsing and AST traversal (MIT)
- [snowflake-connector-python](https://github.com/snowflakedb/snowflake-connector-python) — query history and plan fetch (Apache 2.0)
- Databricks support planned

## Status

Early development. Core fingerprinting logic is working. Clustering, capture
job, and operator review interface are in progress.

## Roadmap

- [x] Sweep fingerprinting
- [x] Refine sub-clustering
- [ ] Snowflake capture job
- [ ] Persistent fingerprint table and clustering logic
- [ ] Operator review interface (Streamlit)
- [ ] Materialization suggestion generator
- [ ] LLM explanation layer
- [ ] Databricks support

## Setup

Run `sql/setup.sql` against your Snowflake account before running the capture job.
Requires a role with access to `SNOWFLAKE.ACCOUNT_USAGE`.

## License

MIT
