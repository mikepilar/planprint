CREATE DATABASE IF NOT EXISTS planprint;
CREATE SCHEMA IF NOT EXISTS planprint.public;

CREATE TABLE IF NOT EXISTS planprint.public.query_fingerprints (
    query_id              VARCHAR        NOT NULL
  , template_hash         VARCHAR        NOT NULL
  , sweep_hash            VARCHAR        NOT NULL
  , refine_hash           VARCHAR        NOT NULL
  , warehouse_name        VARCHAR
  , query_text            VARCHAR
  , topology_json         VARIANT
  , signature_json        VARIANT
  , plan_json             VARIANT
  , projected_columns     VARIANT
  , execution_time_ms     NUMBER
  , bytes_scanned         NUMBER
  , credits_used          FLOAT
  , captured_at           TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
  , PRIMARY KEY (query_id)
);
