# Streaming Elasticsearch Indexes

This document describes the Elasticsearch indexes written by the current speed-layer streaming pipeline after phase 6.

## Summary

| Index | Writer | Purpose |
| --- | --- | --- |
| `jobs_realtime_v1` | `apps/stream_etl/sinks/elasticsearch_sink.py` | Search/filter normalized realtime jobs |
| `realtime_job_counts_10m_v1` | `apps/stream_etl/sinks/jobs_per_10m_sink.py` | 10-minute job-count dashboard |
| `realtime_skill_counts_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` | Hourly skill count trends |
| `realtime_top_skills_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` | Top N skills per hour |
| `realtime_salary_bins_hourly_v1` | `apps/stream_etl/sinks/salary_bins_realtime_sink.py` | Hourly salary distribution |
| `stream_dead_letter_v1` | Not currently written | Planned ES mirror for Kafka `jobs_dead_letter` |

## `jobs_realtime_v1`

Recommended mapping:

```json
{
  "mappings": {
    "dynamic": true,
    "properties": {
      "job_id": { "type": "keyword" },
      "hash_content": { "type": "keyword" },
      "source": { "type": "keyword" },
      "source_url": { "type": "keyword" },
      "event_time": { "type": "date" },
      "ingest_time": { "type": "date" },
      "stream_ingest_time": { "type": "date" },
      "indexed_at": { "type": "date" },
      "title": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "company_name": { "type": "keyword" },
      "city": { "type": "keyword" },
      "location_raw": { "type": "keyword" },
      "category": { "type": "keyword" },
      "level": { "type": "keyword" },
      "employment_type": { "type": "keyword" },
      "experience_months": { "type": "integer" },
      "salary_raw": { "type": "keyword" },
      "salary_min_million": { "type": "double" },
      "salary_max_million": { "type": "double" },
      "salary_avg_million": { "type": "double" },
      "salary_bin": { "type": "keyword" },
      "currency": { "type": "keyword" },
      "skills": { "type": "keyword" },
      "description": { "type": "text" },
      "requirements": { "type": "text" },
      "benefits": { "type": "text" },
      "quality_flags": { "type": "object", "enabled": true }
    }
  }
}
```

Document id: `job_id`.

## `realtime_job_counts_10m_v1`

Recommended mapping:

```json
{
  "mappings": {
    "properties": {
      "bucket_date": { "type": "date" },
      "window_start": { "type": "date" },
      "window_end": { "type": "date" },
      "source": { "type": "keyword" },
      "city": { "type": "keyword" },
      "category": { "type": "keyword" },
      "job_count": { "type": "long" },
      "distinct_company_count": { "type": "long" },
      "updated_at": { "type": "date" }
    }
  }
}
```

Document id: `window_start|source|city|category`.

## `realtime_skill_counts_hourly_v1`

Recommended mapping:

```json
{
  "mappings": {
    "properties": {
      "bucket_date": { "type": "date" },
      "window_start": { "type": "date" },
      "window_end": { "type": "date" },
      "skill": { "type": "keyword" },
      "job_count": { "type": "long" },
      "updated_at": { "type": "date" }
    }
  }
}
```

Document id: `window_start|skill`.

## `realtime_top_skills_hourly_v1`

Recommended mapping:

```json
{
  "mappings": {
    "properties": {
      "bucket_date": { "type": "date" },
      "window_start": { "type": "date" },
      "window_end": { "type": "date" },
      "rank": { "type": "integer" },
      "skill": { "type": "keyword" },
      "job_count": { "type": "long" },
      "updated_at": { "type": "date" }
    }
  }
}
```

Document id: `window_start|rank`.

## `realtime_salary_bins_hourly_v1`

Recommended mapping:

```json
{
  "mappings": {
    "properties": {
      "bucket_date": { "type": "date" },
      "window_start": { "type": "date" },
      "window_end": { "type": "date" },
      "city": { "type": "keyword" },
      "level": { "type": "keyword" },
      "salary_bin": { "type": "keyword" },
      "job_count": { "type": "long" },
      "avg_salary_min_million": { "type": "double" },
      "avg_salary_max_million": { "type": "double" },
      "median_salary_million": { "type": "double" },
      "updated_at": { "type": "date" }
    }
  }
}
```

Document id: `window_start|city|level|salary_bin`.

## `stream_dead_letter_v1`

The current pipeline writes invalid records to Kafka topic `jobs_dead_letter`. If mirrored to Elasticsearch later, use this mapping:

```json
{
  "mappings": {
    "properties": {
      "dead_letter_key": { "type": "keyword" },
      "raw_json": { "type": "text", "index": false },
      "error_reason": { "type": "keyword" },
      "kafka_topic": { "type": "keyword" },
      "kafka_partition": { "type": "integer" },
      "kafka_offset": { "type": "long" },
      "kafka_timestamp": { "type": "date" },
      "created_at": { "type": "date" }
    }
  }
}
```
