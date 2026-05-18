# Cranfield Small Retrieval Dataset

Source: https://github.com/oussbenk/cranfield-trec-dataset

This folder contains a compact Cranfield retrieval test bundle for local XAgent testing.

- `cran.docs.full.1400.jsonl`: all 1400 documents converted to JSONL for direct workspace file indexing.
- `cran.qry.xml`: Cranfield query topics.
- `cranqrel.trec.txt`: qrels in TREC format.
- `xagent-eval-full.jsonl`: 225 XAgent eval cases generated from every Cranfield query. Cases with qrels pass when the response contains any relevant docno from qrels; tool choice is not scored.

For the Eval panel, import `system/eval/cranfield-small/xagent-eval-full.jsonl` as JSONL.
