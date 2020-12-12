CREATE TABLE kv_jsonb (
  path        TEXT COLLATE "C",
  key         TEXT COLLATE "C",
  value       JSONB,
  CONSTRAINT kv_jsonb_pkey PRIMARY KEY (path, key)
);

CREATE TABLE kv_json (
  path        TEXT COLLATE "C",
  key         TEXT COLLATE "C",
  value       JSON,
  CONSTRAINT kv_json_pkey PRIMARY KEY (path, key)
);

CREATE EXTENSION hstore;

CREATE TABLE kv_hstore (
  path        TEXT COLLATE "C",
  key         TEXT COLLATE "C",
  value       HSTORE,
  CONSTRAINT kv_hstore_pkey PRIMARY KEY (path, key)
);

---- create above / drop below ----

--drop table schedule;
