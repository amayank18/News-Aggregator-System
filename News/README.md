# News Aggregation System

A system design for a large-scale, near-real-time news aggregation platform. The design ingests articles from multiple sources, removes duplicates, groups related coverage into stories, ranks those stories, and serves topic-based feeds efficiently.

## Architecture

```text
RSS / Websites / Social Sources
              |
           Ingestion
              |
            Kafka
              |
         Normalization
              |
       Vector Deduplication
              |
      Online Story Clustering
              |
        Cluster Labelling
              |
            Ranking
              |
   Redis sorted sets by topic
              |
        Feed Service
       fan-out-on-read
```

Each processing stage is decoupled through Kafka so that stages can be scaled independently and failures can be isolated.

## Core Components

- **Ingestion:** Collects articles and metadata from RSS feeds, websites, and social sources.
- **Normalization:** Produces a consistent article representation and removes malformed or incomplete data.
- **Deduplication:** Uses embeddings and vector similarity to detect repeated coverage of the same article or event.
- **Online clustering:** Compares new articles with active story centroids and groups related coverage.
- **Labelling:** Generates a readable label for each story cluster.
- **Ranking:** Combines freshness, popularity, source reliability, and other signals into a story score.
- **Redis feed indexes:** Maintains top stories in sorted sets for each topic.
- **Feed service:** Merges and reranks topic-level results at read time to build a user feed.

## Scale Assumptions

The initial sizing model assumes:

| Metric | Estimate |
| --- | ---: |
| Daily active users | 10 million |
| Feed requests per day | 150 million |
| Average feed QPS | 1,750 |
| Peak feed QPS | 5,000-9,000 |
| Raw articles per day | 5 million |
| Unique articles after deduplication | 3.25 million/day |
| New story clusters per day | 300,000 |
| Feed cache RAM | 100-150 GB provisioned |
| Article and vector storage growth | 45-50 TB/year with overhead |

These numbers are planning estimates, not production requirements. They should be revisited using real traffic, article size, cache hit rate, and consumer-lag measurements.

## Key Tradeoffs

### Fan-out-on-read

Feeds are assembled from topic-level Redis indexes when requested instead of maintaining a precomputed feed for every user. This avoids large write amplification for a content-heavy product, at the cost of merge, rerank, and diversification work on each read.

### Asynchronous processing

The multi-stage Kafka pipeline provides fault isolation and independent scaling. The tradeoff is that freshness depends on the slowest stage, so the platform should measure end-to-end latency and consumer lag rather than treating the system as strictly real-time.

### Embedding similarity

A similarity threshold controls the balance between merging paraphrased coverage and keeping distinct stories separate. The threshold should be configurable and monitored, with title or URL matching available as a secondary signal.

### Centroid-based clustering

Comparing new articles with story centroids is more tractable than comparing against every article. Long-running stories can still experience centroid drift, so periodic re-anchoring or bounded centroid updates may be needed.

### Primary database and vector database

Using a document store for article data and a vector database for similarity search lets each system serve its primary workload. Because the writes are not atomic across both systems, ingestion should use idempotent keys, retries, and an outbox or reconciliation process.

## Failure Modes to Monitor

- Duplicate stories leaking into feeds when similarity checks miss paraphrases.
- Hot clusters causing repeated rank recomputations during breaking news.
- Kafka consumer lag cascading into downstream freshness delays.
- Redis restarts causing a cache miss storm against the primary database.
- Reliability-score service failures blocking or degrading ranking.
- Low-quality source floods artificially inflating source-count popularity.

Useful mitigations include secondary deduplication signals, batched ranking updates, per-stage lag alerts, Redis warm-up jobs, cached default reliability scores, and reputation-weighted source counts.

## Open Design Questions

- What are the target freshness and availability SLOs?
- Which article and story fields belong in the primary database versus Redis?
- How will similarity thresholds be tuned and evaluated offline?
- How will source reliability be calculated and refreshed?
- What retention policy applies to raw articles, embeddings, and completed clusters?
- Which personalization signals should be applied during feed assembly?

## Related Notes

- [BOE.md](BOE.md) - Capacity estimates and component-level sizing.
- [Failure-&-Tradeoffs.md](Failure-&-Tradeoffs.md) - Detailed tradeoffs, failure modes, and mitigations.
