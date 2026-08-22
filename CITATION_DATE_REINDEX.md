# Citation-date v2 reindex runbook

Citation-date v2 prefers the Verbatim F8/13pt-bold cite span, ignores access-date text, accepts evidence years only through the current year plus one, and maps modern shortened years such as `22` to `2022`.

## Zero-downtime rollout

1. Deploy the parser/API change while leaving the existing OpenSearch alias unchanged. Keep `CITATION_DATE_CAPABILITY_VERSION=1` so the evidence-date UI stays disabled.
2. Create a new versioned index (for example `cards-citation-v2-YYYYMMDD`) with the current production mappings and analyzers.
3. Re-parse the immutable source documents into the new index and a versioned DynamoDB export/table. Do not update the existing index in place.
4. Compare document count, unique card IDs, search smoke tests, and date distributions. Explicitly audit unknown dates, future dates, access-date leakage, and a sample of F8 cite spans.
5. Replay writes that arrived after the backfill watermark, then pause ingestion briefly and replay the final delta.
6. Atomically switch the read alias to the v2 index, then set `CITATION_DATE_CAPABILITY_VERSION=2`. Keep the old index read-only for rollback.
7. Verify `/query` and `/card`, date boundaries, pagination, and the DSPN capability check. Only then enable the evidence-date UI.
8. Retain the old index through the agreed rollback window; remove it only after monitoring shows no regression.

Rollback is an atomic alias switch to the prior index. The capability endpoint must report the version actually serving reads, not merely the version of the deployed application code.
