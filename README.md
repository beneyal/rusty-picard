# Rusty PICARD

This is a Rust implementation of [PICARD: Parsing Incrementally for Constrained Auto-Regressive Decoding from Language Models](https://aclanthology.org/2021.emnlp-main.779/)
built to incrementally parse [QPL](https://github.com/bgunlp/qpl) instead of SQL.

## What is this for? 🤔

When a language model generates structured output token-by-token, nothing stops it
from producing something syntactically broken or semantically nonsensical (referencing
a column that doesn't exist, joining on a key that isn't there, etc.). PICARD attaches
to the decoder's beam search and, at each step, rejects candidate tokens that cannot
lead to a valid continuation.

Rusty PICARD does the same thing — but for [QPL](https://github.com/bgunlp/qpl), an
intermediate representation that decomposes complex SQL
into a sequence of simple, numbered, single-operation lines. QPL's structure is
friendlier to incremental validation than raw SQL, which makes constrained decoding
both faster and more precise.

## How it works 🧩

The server exposes an HTTP API. Your decoder (in Python, or anywhere else) calls into
it on each generation step:

1. Register one or more SQL schemas (table/column names, types, foreign keys, primary keys).
2. Register the tokenizer your model uses, so the parser can detokenize beam-search candidates.
3. On every decoding step, send the current `input_ids` plus the top-k candidate `top_tokens`.
   Rusty PICARD detokenizes each `(prefix + candidate)` pair, runs it through an
   incremental QPL parser, and tells you whether the result is:
   - `complete` — a fully valid QPL program,
   - `partial` — a valid prefix that could still grow into a complete program,
   - `failure` — already unparseable; the decoder should mask this token out.

Candidate evaluation is fanned out across cores with [`rayon`](https://crates.io/crates/rayon),
so feeding a wide beam stays cheap.

### Type checking

Pass schemas with column types and the parser will additionally enforce type-level
constraints (e.g. predicates over a `Number` column must compare against a number).
This is the `with_type_checking` knob threaded through the parser.

## API 📡

The service listens on `0.0.0.0:8081`.

| Method | Path         | Body                                           | Purpose                                |
|--------|--------------|------------------------------------------------|----------------------------------------|
| GET    | `/health`    | —                                              | Liveness probe.                        |
| GET    | `/debug`     | —                                              | Dump current server state to the log.  |
| POST   | `/schema`    | `SqlSchema` JSON                               | Register a database schema.            |
| POST   | `/tokenizer` | A serialized `tokenizers` JSON (as text)       | Register the HF tokenizer to use.      |
| POST   | `/validate`  | `{ "qpl": "..." }`                             | One-shot: is this full QPL string valid? |
| POST   | `/parse`     | `{ "input_ids": [[...]], "top_tokens": [[...]] }` | Batched incremental check used during decoding. |

`/parse` returns one `BatchFeedResult` per `(batch, top_token)` pair, each tagged with
`complete`, `partial`, or `failure`.

## Running 🚀

```bash
cargo run --release
```

The server binds to port `8081`. Logging is via `tracing-subscriber`; set `RUST_LOG`
to taste:

```bash
RUST_LOG=rusty_picard=debug,tower_http=debug cargo run --release
```

## Inside the parser 🔬

The grammar is built with [`winnow`](https://crates.io/crates/winnow)'s partial-input
parsers, which is what makes incremental "is this a valid prefix?" checking natural:
an incomplete-but-consistent input cleanly surfaces as `ErrMode::Incomplete`.

Each QPL operation has its own parser module under `src/parser/`:

- `scan` — table reads, with optional predicate and `Distinct`
- `filter` — row filtering over a single input
- `aggregate` — group-by + aggregate expressions (`Sum`, `Min`, `Max`, `Count`, `Avg`)
- `join` / `intersect` / `except` / `union` — binary set operations
- `sort` / `top` / `top_sort` — ordering and limiting

The parser threads a `QplEnvironment` (active schema + line-by-line state: known indices,
output schemas, etc.) through every combinator, so a downstream line like
`Join [ #1, #2 ]` can be checked against the actual columns produced upstream.

## Repo layout 🗂️

```
src/
├── main.rs        # axum server, /parse fan-out, tokenizer plumbing
├── api.rs         # request/response types and shared server state
├── domain.rs      # QPL AST: Operation, Predicate, Column, Table, QplState, ...
├── schemas.rs     # test fixtures (e.g. concert_singer)
├── parser.rs      # top-level QPL line/program parsers
└── parser/        # one module per operation + shared utilities
```

## Tests 🧪

```bash
cargo test
```

The parser module ships with positive and negative QPL examples drawn from real Spider
queries, plus a partial-input test asserting that a truncated QPL string returns
`Incomplete` rather than a hard failure — exactly the property constrained decoding
depends on.
