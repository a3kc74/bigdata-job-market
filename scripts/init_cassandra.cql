CREATE KEYSPACE IF NOT EXISTS job_market_speed
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE job_market_speed;

CREATE TABLE IF NOT EXISTS realtime_job_counts_10m (
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  source text,
  city text,
  category text,
  job_count bigint,
  distinct_company_count bigint,
  updated_at timestamp,
  PRIMARY KEY ((bucket_date), window_start, source, city, category)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS realtime_skill_counts_hourly (
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  skill text,
  job_count bigint,
  updated_at timestamp,
  PRIMARY KEY ((bucket_date, window_start), skill)
);

CREATE TABLE IF NOT EXISTS realtime_top_skills_hourly (
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  rank int,
  skill text,
  job_count bigint,
  updated_at timestamp,
  PRIMARY KEY ((bucket_date, window_start), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

CREATE TABLE IF NOT EXISTS realtime_salary_bins_hourly (
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  city text,
  level text,
  salary_bin text,
  job_count bigint,
  avg_salary_min_million double,
  avg_salary_max_million double,
  median_salary_million double,
  updated_at timestamp,
  PRIMARY KEY ((bucket_date, window_start), city, level, salary_bin)
);

CREATE TABLE IF NOT EXISTS jobs_realtime_by_id (
  job_id text PRIMARY KEY,
  hash_content text,
  source text,
  source_url text,
  event_time timestamp,
  ingest_time timestamp,
  stream_ingest_time timestamp,
  title text,
  company_name text,
  city text,
  category text,
  level text,
  employment_type text,
  experience_months int,
  salary_raw text,
  salary_min_million double,
  salary_max_million double,
  salary_avg_million double,
  salary_bin text,
  currency text,
  skills list<text>,
  updated_at timestamp
);

CREATE TABLE IF NOT EXISTS stream_dead_letter_by_day (
  bucket_date date,
  topic text,
  partition int,
  offset bigint,
  raw_json text,
  error_reason text,
  created_at timestamp,
  PRIMARY KEY ((bucket_date), created_at, topic, partition, offset)
) WITH CLUSTERING ORDER BY (created_at DESC);
