from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS metadata_catalog")

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata_catalog.ingest
(
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name     STRING NOT NULL,
    source_entity   STRING NOT NULL,
    record_count    INT NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    data VARIANT NOT NULL
    
    PARTITIONED BY (source_name)
);""")