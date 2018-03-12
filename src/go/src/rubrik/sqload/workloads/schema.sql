DROP DATABASE IF EXISTS sqload CASCADE;
CREATE DATABASE sqload;

-- bg_scan_int_key_generator
CREATE TABLE sqload.bg_scan_generator_int_keyed (
  job_config text,
  job_id int,
  job_name text,
  node_id int,
  PRIMARY KEY(job_id),
  INDEX node_id_idx (node_id),
  FAMILY f1 (job_id, job_name, node_id),
  FAMILY f2 (job_config)
);

-- micro_benchmark
CREATE TABLE sqload.micro_benchmark (
  job_config text,
  job_id int,
  job_name text,
  node_id int,
  PRIMARY KEY(job_id),
  FAMILY f1 (job_id, job_name, node_id),
  FAMILY f2 (job_config)
);

-- files
CREATE TABLE sqload.files_sql (
  birth_time int,
  directory_spec text,
  lock text,
  open_heartbeat_time int,
  parent_uuid_hint text,
  stripe_id int,
  stripe_metadata text,
  symlink_target text,
  token__uuid int,
  uuid text,
  PRIMARY KEY(token__uuid, uuid, stripe_id)
);
CREATE TABLE sqload.files_sql__child_map__map (
  map__key text,
  map__val text,
  stripe_id int,
  token__uuid int,
  uuid text,
  PRIMARY KEY(token__uuid, uuid, stripe_id, map__key)
);
CREATE TABLE sqload.files_sql__static (
  ctime int,
  file_spec text,
  history text,
  history_serialized text,
  internal_timestamp int,
  last_modifier text,
  linearizer int,
  metadata_version int,
  mode int,
  mtime int,
  size int,
  state int,
  token__uuid int,
  uuid text,
  PRIMARY KEY(token__uuid, uuid)
);
