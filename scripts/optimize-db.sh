#!/bin/bash
# optimize-db.sh — Add pre-computed cache tables to plaindoctor.db
# Run on Titan: bash optimize-db.sh /opt/portals/data/sqlite/plaindoctor.db
# Or locally: bash optimize-db.sh ./data/plaindoctor.db
#
# Creates 3 cache tables to eliminate expensive queries:
#   1. sitemap_pages        — Keyset pagination boundaries (eliminates OFFSET scanning)
#   2. specialty_top_cities — Top cities per specialty (eliminates GROUP BY on 7M rows)
#   3. city_top_specialties — Top specialties per city (eliminates GROUP BY on 7M rows)

set -euo pipefail

DB="${1:?Usage: optimize-db.sh <path-to-plaindoctor.db>}"

if [ ! -f "$DB" ]; then
  echo "ERROR: Database not found at $DB"
  exit 1
fi

echo "Optimizing $DB..."
echo "DB size before: $(du -h "$DB" | cut -f1)"

# Step 1: sitemap_pages
echo "Step 1/5: Creating sitemap_pages..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS sitemap_pages;
CREATE TABLE sitemap_pages (
  page INTEGER PRIMARY KEY,
  start_npi TEXT NOT NULL
);
INSERT INTO sitemap_pages
SELECT
  ((row_num - 1) / 50000) + 1 AS page,
  npi AS start_npi
FROM (
  SELECT npi, ROW_NUMBER() OVER (ORDER BY npi) AS row_num
  FROM providers
)
WHERE (row_num - 1) % 50000 = 0;
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM sitemap_pages') pages created"

# Step 2: Compute specialty×city counts into temp, rank, keep top 30
echo "Step 2/5: Creating specialty_top_cities..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS specialty_top_cities;
CREATE TABLE specialty_top_cities (
  specialty_code TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  city_slug TEXT,
  provider_count INTEGER NOT NULL
);

-- Use window function to rank and filter in one pass (no O(n²) correlated subquery)
INSERT INTO specialty_top_cities (specialty_code, city, state, city_slug, provider_count)
SELECT specialty_code, city, state, city_slug, provider_count
FROM (
  SELECT
    agg.specialty_code, agg.city, agg.state, c.slug AS city_slug, agg.cnt AS provider_count,
    ROW_NUMBER() OVER (PARTITION BY agg.specialty_code ORDER BY agg.cnt DESC) AS rn
  FROM (
    SELECT specialty_code, city, state, COUNT(*) AS cnt
    FROM providers
    GROUP BY specialty_code, city, state
  ) agg
  LEFT JOIN cities c ON c.city = agg.city AND c.state = agg.state
)
WHERE rn <= 30;

CREATE INDEX idx_stc_spec ON specialty_top_cities(specialty_code, provider_count DESC);
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM specialty_top_cities') rows"

# Step 3: Compute city×specialty counts, rank, keep top 25
echo "Step 3/5: Creating city_top_specialties..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS city_top_specialties;
CREATE TABLE city_top_specialties (
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  specialty_code TEXT NOT NULL,
  specialty TEXT NOT NULL,
  provider_count INTEGER NOT NULL
);

INSERT INTO city_top_specialties (city, state, specialty_code, specialty, provider_count)
SELECT city, state, specialty_code, specialty, provider_count
FROM (
  SELECT
    city, state, specialty_code, specialty, COUNT(*) AS provider_count,
    ROW_NUMBER() OVER (PARTITION BY city, state ORDER BY COUNT(*) DESC) AS rn
  FROM providers
  GROUP BY city, state, specialty_code
)
WHERE rn <= 25;

CREATE INDEX idx_cts_city ON city_top_specialties(city, state, provider_count DESC);
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM city_top_specialties') rows"

# Step 4: ANALYZE (update query planner stats)
echo "Step 4/5: Running ANALYZE..."
sqlite3 "$DB" "ANALYZE;"

# Step 5: VACUUM + journal mode
echo "Step 5/5: VACUUM + journal_mode=DELETE..."
sqlite3 "$DB" "VACUUM; PRAGMA journal_mode=DELETE;"

echo "DB size after: $(du -h "$DB" | cut -f1)"
echo "Done! Cache tables created."
