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
  tested NUMERIC,
  tested_with_scores NUMERIC,
  mean_scale_score NUMERIC,
  pct_exceeded NUMERIC,
  cnt_exceeded NUMERIC,
  pct_met NUMERIC,
  cnt_met NUMERIC,
  pct_met_and_above NUMERIC,
  cnt_met_and_above NUMERIC,
  pct_nearly_met NUMERIC,
  cnt_nearly_met NUMERIC,
  pct_not_met NUMERIC,
  cnt_not_met NUMERIC,
  county_name TEXT,
  county_code TEXT,
  district_name TEXT,                 -- keep raw names for now
  district_code TEXT,
  school_name TEXT,                    -- keep raw names for now
  school_code TEXT
);

-- Composite index for common query patterns (year, subject, grade, subgroup)
CREATE INDEX IF NOT EXISTS idx_scores_keys
  ON analytics.fact_scores (year_key, subject, grade, subgroup);

-- Index for school/district lookups
CREATE INDEX IF NOT EXISTS idx_scores_location
  ON analytics.fact_scores (county_code, district_code, school_code);

-- Index for district-level queries
CREATE INDEX IF NOT EXISTS idx_scores_district
  ON analytics.fact_scores (district_code, year_key, subject);

-- Index for school-level queries
CREATE INDEX IF NOT EXISTS idx_scores_school
  ON analytics.fact_scores (school_code, year_key, subject);

-- Index for subgroup analysis
CREATE INDEX IF NOT EXISTS idx_scores_subgroup
  ON analytics.fact_scores (subgroup, year_key, subject);

-- Partial index for non-null school names (for text searches)
CREATE INDEX IF NOT EXISTS idx_scores_school_name
  ON analytics.fact_scores (school_name)
  WHERE school_name IS NOT NULL;