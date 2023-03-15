-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Sample notebook

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS discoverx_sample;
USE CATALOG discoverx_sample;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS discoverx_sample.sample_datasets

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS discoverx_sample.sample_datasets.department
(
  deptcode   INT,
  deptname  STRING,
  location  STRING
);

INSERT OVERWRITE TABLE discoverx_sample.sample_datasets.department VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS discoverx_sample.sample_datasets.cyber_data
(
  ip_v4_address STRING,
  ip_v6_address STRING,
  mac_address   STRING
);

INSERT OVERWRITE TABLE discoverx_sample.sample_datasets.cyber_data VALUES
  ('1.2.3.4', '64:ff9b::192.0.2.33', 'FF:FF:FF:FF:FF:FF'),
  ('10.20.30.40', '1:2:3:4:5:6:7:8', '00:00:5e:00:53:af'),
  ('255.255.255.255', '1::', '00:00:00:00:00:00'),
  ('0.0.0.0', '1::5:6:7:8', '00:00:00:00:00:00'),
  ('0.0.0.0', '1::3:4:5:6:7:8', '00:00:00:00:00:00'),
  ('0.0.0.0', 'fe80::7:8%eth0', '00:00:00:00:00:00'),
  ('0.0.0.0', '::255.255.255.255', '00:00:00:00:00:00');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS discoverx_sample.sample_datasets.cyber_data_2
(
  source_address      STRING,
  destination_address STRING,
  content             STRING
);

INSERT OVERWRITE TABLE discoverx_sample.sample_datasets.cyber_data_2 VALUES
  ('1.2.3.4', '10.2.3.4', '{"key": "val_1", "key2": 1}'),
  ('1.2.3.5', '10.2.3.4', '{"key": "val_2", "key2": 2}'),
  ('1.2.3.6', '10.2.3.4', '{"key": "val_3", "key2": 3}'),
  ('0.0.0.0', '255.255.255.255', '00:00:00:00:00:00');

-- COMMAND ----------


