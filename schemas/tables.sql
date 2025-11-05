-- tables.sql.tmpl

CREATE TABLE IF NOT EXISTS `${project_id}.analytics.my_table_1` (
  id INT64,
  name STRING
);

CREATE TABLE IF NOT EXISTS `${project_id}.analytics.my_table_2` (
  event_time TIMESTAMP,
  value FLOAT64
);
