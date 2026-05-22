# File Index Semantic Search Spec

## Problem

`file_search` currently uses SQLite FTS5 and path matching. That is strong for exact symbols, file names, and literal text, but weak when the user describes intent with different wording than the indexed files.

## Goals

- Add optional chunk-level semantic retrieval on top of the existing SQLite index.
- Keep the current FTS5/path behavior working without extra dependencies or configuration.
- Store all index data in `<ctx.cwd>/runtime/file_index.sqlite3`.
- Use OpenAI-compatible embeddings only when explicitly enabled.
- Return semantic failures as diagnostic metadata while preserving keyword results.

## Non-Goals

- No background indexer.
- No external vector database.
- No LLM reranker.
- No cross-workspace embedding cache.

## Contracts

- `refresh_file_index(..., semantic=True)` may build chunks and embeddings when configuration enables it.
- `search_file_index(..., mode="keyword|semantic|hybrid")` defaults to `hybrid`.
- `path_only=True` disables semantic retrieval.
- Existing match fields remain: `path`, `score`, `match_type`, `line`, `snippet`, `size_bytes`, `mtime`.
- Optional fields may include `start_line`, `end_line`, `signals`, and `semantic_status`.
- Search results are still candidates; callers must use `file_read` before relying on exact content or modifying files.

## Configuration

Embedding is disabled unless either `file_index_embedding.enabled=true` is passed from the first session config or `XAGENT_FILE_INDEX_EMBEDDING=1` is set.

The supported OpenAI-compatible configuration is:

```json
{
  "file_index_embedding": {
    "enabled": true,
    "apikey": "optional",
    "apibase": "https://example.com/v1/embeddings",
    "model": "embedding-model",
    "dimension": 1536,
    "batch_size": 32,
    "timeout": 60
  }
}
```

## Error Handling

- Missing configuration, missing `sqlite-vec`, or embedding HTTP failures must not make `file_search` fail.
- Model or dimension changes rebuild semantic tables only.
- Deleted files remove FTS rows, chunk rows, embedding rows, and vector rows.

## Tests

- Default keyword behavior remains compatible.
- Semantic search works with a fake provider.
- Hybrid search merges keyword and semantic candidates.
- Root filtering applies before semantic limit.
- Missing provider or provider failure degrades to keyword results with `semantic_status`.
