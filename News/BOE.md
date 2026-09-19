Nice diagram — I can see the full pipeline: multi-source ingestion (RSS/website/social) → Kafka → Normalization → Dedup (vector similarity via VectorDB) → Online Clustering → Cluster Labelling → Ranking (weighted score) → Redis sorted sets per topic → Feed Service (fan-out-on-read).

I'll state my assumptions explicitly since the diagram doesn't specify numbers, then work through the math.

## Assumptions (typical for a "millions of users/articles" scale news app)

| Parameter | Assumption |
|---|---|
| Total registered users | 50M |
| DAU/MAU ratio | ~20% (news apps are used less daily than social apps) |
| **DAU** | **10M** |
| Feed opens per user/day | 5 sessions |
| Pages scrolled per session | 3 (`GET /feed`) |
| Raw articles ingested/day | 5M (across RSS + website + social) |
| Duplicate rate | ~30–40% (same story from many sources) |
| Avg cluster size | ~10–12 articles/story |
| Topics/categories | ~50 (sports, finance, tech, politics, etc.) |

---

## 1. Traffic / QPS

- Feed requests/day = 10M DAU × 5 sessions × 3 pages = **150M requests/day**
- Avg QPS = 150M / 86,400 ≈ **~1,750 QPS**
- News traffic is spiky (breaking news, morning/evening peaks) → peak factor 3–5x
- **Peak QPS ≈ 5,000–9,000**

## 2. Ingestion volume

- Raw articles: 5M/day → avg **~58 articles/sec**, peak (breaking news bursts) **300–500/sec**
- After dedup (~35% dupes): **~3.25M unique articles/day**
- Clusters (stories) created/day: 3.25M / ~11 avg cluster size ≈ **~300K stories/day**

## 3. Storage estimate

**a) Article store (DynamoDB — normalized articles)**
- Size/article ≈ title(0.1KB) + summary(0.5KB) + body(5KB) + metadata/URLs(0.5KB) ≈ **~6KB**
- 5M articles/day × 6KB ≈ **30 GB/day** → **~11 TB/year** (raw)
- With DynamoDB's typical 3x replication → **~33 TB/year**

**b) Vector DB (Pinecone/Weaviate — for dedup embeddings)**
- 768-dim float32 vector = 3KB + index/metadata overhead (~1.7x) ≈ **~5KB/article**
- 5M/day × 5KB ≈ **25 GB/day** → **~9 TB/year**

**c) Cluster store (DynamoDB — story/cluster metadata)**
- Centroid vector + article-id list + label + score ≈ **~8KB/cluster**
- 300K clusters/day × 8KB ≈ **2.4 GB/day** → **~0.9 TB/year**

**Total: ~57 GB/day raw ≈ ~20 TB/year, ~45–50 TB/year with replication/indexing overhead**

## 4. RAM (Redis) estimate

**a) Ranking sorted sets** (per-topic top stories)
- 50 topics × top 2,000 stories × ~100 bytes/member (Redis ZSET overhead) ≈ **~10 MB** — negligible

**b) Feed/response caching** (cache miss → DynamoDB, per diagram)
- If caching a computed feed page per active user: 20 articles × ~200B (id+snippet+score) ≈ 4KB/user
- 10M DAU × 4KB ≈ **~40 GB**

**c) Hot article metadata cache** (top trending articles served without DB hit)
- Top ~1M articles × 2KB (denormalized, no full body) ≈ **~2 GB**

**Total Redis: ~45 GB useful data → with replication (2–3x) and headroom, provision ~100–150 GB cluster RAM**

## 5. Kafka throughput

Each article fans out through: ingest topic → dedup-job topic → cluster-job topic → label-job topic (~4 hops)
- Avg messages/sec ≈ 58 × 4 ≈ **~230 msg/sec**, peak **~2,000 msg/sec**
- Avg message size ~5–7KB (article text/vector payload) → **~1.5 MB/s avg, ~14 MB/s peak** — comfortably within a modest Kafka cluster (a handful of brokers/partitions)

## 6. DB read/write load

- **Writes:** ~58/sec avg (500 peak) into article DB + similar order into vector DB + cluster DB + rank updates → a few hundred writes/sec peak, combined
- **Reads (DynamoDB, on Redis cache miss):** assuming ~95% cache hit ratio on feed reads → DB read QPS ≈ 5% × 1,750 avg ≈ **~90 QPS avg**, **~250–450 QPS peak**

## 7. Network egress (Feed API)

- Avg feed response ≈ 20 articles × ~300B (summary payload, image via CDN URL only) ≈ **~6–8 KB**
- 150M requests/day × 8KB ≈ **~1.2 TB/day** egress
- Avg bandwidth ≈ **~14 MB/s**, peak **~50–70 MB/s**

---

## Summary table

| Metric | Estimate |
|---|---|
| DAU | 10M |
| Peak feed QPS | ~5,000–9,000 |
| Raw article ingestion | 5M/day (~58/sec avg, ~500/sec peak) |
| Unique articles after dedup | ~3.25M/day |
| Stories/clusters created | ~300K/day |
| Article + vector storage growth | ~50–60 GB/day → ~20 TB/year (raw), ~45–50 TB/year w/ replication |
| Redis RAM | ~45 GB useful → provision ~100–150 GB |
| Kafka throughput | ~1.5 MB/s avg, ~14 MB/s peak |
| DB read QPS (post-cache) | ~90 avg / ~250–450 peak |
| Feed API egress | ~1.2 TB/day, ~14–70 MB/s |

A few callouts on the design itself while I was reading it:
- The **95%-similarity threshold + centroid comparison** for online clustering is O(n) per new article against active centroids unless you shard by recency/topic first — at 500 articles/sec peak, you'll want an ANN index (HNSW) in the vector DB rather than brute-force centroid comparison, or you'll bottleneck there before anywhere else.
- Your bottleneck is more likely to be the **clustering + ranking recompute chain** than raw ingestion — since a new article touching a cluster triggers a rank recompute for that whole story, high-velocity breaking news (100s of sources within minutes) could cause hot-cluster contention.

Want me to turn this into a formatted doc/PDF, or dig deeper into any one component (e.g. sizing the vector DB ANN index, or the Kafka partition/consumer group layout)?