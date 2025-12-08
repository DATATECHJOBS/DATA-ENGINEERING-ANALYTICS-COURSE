create or replace storage integration s3_init
    type = external_stage
    storage_provider = s3
    enabled = true
    storage_aws_role_arn = ''
    storage_allowed_locations = ('s3://')
    comment = 'I am connecting to AWS S3';

DESC integration s3_init;

  --create reusable file format
    CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6';

    CREATE OR REPLACE stage crypto_stage
    url = 's3://bitcoindatapipelineproject/refined_layer/'
    storage_integration = s3_init
    file_format = csv_fileformat;

    CREATE OR REPLACE TABLE tbl_coin_metadata (
    coin_id STRING,
    symbol STRING,
    name STRING,
    image_url STRING,
    market_cap_rank INT,
    max_supply FLOAT,
    extracted_at TIMESTAMP
);

CREATE OR REPLACE TABLE tbl_coin_market (
    coin_id STRING,
    price_usd FLOAT,
    market_cap_usd FLOAT,
    volume_usd FLOAT,
    high_24h_usd FLOAT,
    low_24h_usd FLOAT,
    price_change_24h FLOAT,
    price_change_pct_24h FLOAT,
    market_cap_change_24h FLOAT,
    market_cap_change_pct_24h FLOAT,
    extracted_at TIMESTAMP
);
COPY INTO tbl_coin_metadata
FROM @crypto_stage/meta_data/meta_data_transformed_2025-08-18/run-1755544572142-part-r-00000;

COPY INTO tbl_coin_market
FROM @crypto_stage/market_data/market_data_transformed_2025-08-18/run-1755544279098-part-r-00000;
----------
----a schema is like a folder, keep similar stages, tables, pipes
CREATE OR REPLACE SCHEMA snowpipes;

CREATE OR REPLACE pipe crypto_db.snowpipes.tbl_metadata_pipe
auto_ingest = TRUE
AS
COPY INTO crypto_db.public.tbl_coin_metadata
FROM @crypto_db.public.crypto_stage/meta_data;

DESC pipe snowpipes.tbl_metadata_pipe;



SELECT count(*) FROM tbl_coin_metadata;

SELECT SYSTEM$PIPE_STATUS('snowpipes.tbl_metadata_pipe')

DROP SCHEMA snowpipes;

metadata_added_event

CREATE OR REPLACE pipe crypto_db.snowpipes.tbl_marketdata_pipe
auto_ingest = TRUE
AS
COPY INTO crypto_db.public.tbl_coin_marketdata
FROM @crypto_db.public.crypto_stage/market_data;


DESC pipe snowpipes.tbl_marketdata_pipe;






































