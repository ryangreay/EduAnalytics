CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_year (
  year_key INT PRIMARY KEY,           -- e.g., 2022 for AY 2022–23
  label TEXT
);

CREATE TABLE IF NOT EXISTS analytics.fact_scores (
  id BIGSERIAL PRIMARY KEY,
  year_key INT REFERENCES analytics.dim_year(year_key),
  subject TEXT CHECK (subject IN ('Math','ELA')),
  subgroup TEXT,                      -- Student Group ID or resolved label later
  grade TEXT,                         -- Grade text/number as provided
  tested INT,
  tested_with_scores INT,
  mean_scale_score NUMERIC,
  pct_exceeded NUMERIC,
  cnt_exceeded INT,
  pct_met NUMERIC,
  cnt_met INT,
  pct_met_and_above NUMERIC,
  cnt_met_and_above INT,
  pct_nearly_met NUMERIC,
  cnt_nearly_met INT,
  pct_not_met NUMERIC,
  cnt_not_met INT,
  district_name TEXT,                 -- keep raw names for now
  school_name TEXT                    -- keep raw names for now
);

CREATE INDEX IF NOT EXISTS idx_scores_keys
  ON analytics.fact_scores (year_key, subject, grade, subgroup);