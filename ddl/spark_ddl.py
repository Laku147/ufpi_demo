from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE DATABASE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw.line_item
(
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id    INT       NOT NULL,
    promotion_ids ARRAY<INT>,
    quantity      INT       NOT NULL,
    invoice_id    BIGINT NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw.address
(
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    street          VARCHAR(255)                        NOT NULL,
    state           VARCHAR(130),
    country         VARCHAR(130)                        NOT NULL,
    zip             VARCHAR(20),
    timezone_offset SMALLINT,
    updated_at      TIMESTAMP NOT NULL
);""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw.customer
(
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_name         VARCHAR(255) NOT NULL,
    email                VARCHAR(100) NOT NULL,
    phone                VARCHAR(20),
    industry             VARCHAR(50),
    delivery_address_id  BIGINT,
    corporate_address_id BIGINT,
    created_at           TIMESTAMP    NOT NULL,
    updated_at           TIMESTAMP    NOT NULL,

    CONSTRAINT fk_customer_delivery_address
        FOREIGN KEY (delivery_address_id) REFERENCES bronze_ufpi.ufpi_db_demo_raw.address (id),
    CONSTRAINT fk_customer_corporate_address
        FOREIGN KEY (corporate_address_id) REFERENCES bronze_ufpi.ufpi_db_demo_raw.address (id)
);""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw.promotion
(
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    promotion_description VARCHAR(255)   NOT NULL,
    start_time            TIMESTAMP      NOT NULL,
    end_time              TIMESTAMP      NOT NULL,
    discount_amount       NUMERIC(10, 2) NOT NULL,
    updated_at            TIMESTAMP
);""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_ufpi.ufpi_db_demo_raw.orders
(
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     BIGINT        NOT NULL,

    line_items      ARRAY<STRING>         NOT NULL,
    shipping_status VARCHAR(50),
    payment_type    VARCHAR(50),

    total_price     NUMERIC(10, 2) NOT NULL,
    total_tax       NUMERIC(10, 2) NOT NULL,
    shipping_cost   NUMERIC(10, 2),
    currency        VARCHAR(3),

    created_at      TIMESTAMP      NOT NULL,
    updated_at      TIMESTAMP      NOT NULL,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES bronze_ufpi.ufpi_db_demo_raw.customer (id)
);""")