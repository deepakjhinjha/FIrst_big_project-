-- ==========================================
-- 1. SETUP (Bronze & File Format)
-- ==========================================
CREATE OR REPLACE DATABASE snow_project;
use snow_project;
CREATE OR REPLACE SCHEMA pipe_schema;
USE SCHEMA pipe_schema;

CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://ritzzz/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (TYPE = JSON);  
list @my_s3_stage;


CREATE OR REPLACE TABLE customer_raw (
    raw_content VARIANT,
    file_name STRING,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM customer_stream ON TABLE customer_raw;

-- ==========================================
-- 2. SILVER LAYER (New Date Columns Added)
-- ==========================================
CREATE OR REPLACE TABLE customer_final (
    CUSTOMER_ID STRING,
    CUSTOMER_NAME STRING,
    EMAIL_ID STRING,
    PHONE_NUMBER STRING,
    CITY STRING,
    STATE STRING,
    COUNTRY STRING,
    CUSTOMER_SEGMENT STRING,
    CUSTOMER_STATUS STRING,
    CREDIT_RATING STRING,
    SOURCE_SYSTEM STRING,
    FILE_NAME STRING,
    -- Time Columns
    LOAD_YEAR INT,
    LOAD_MONTH INT,
    LOAD_DAY INT,
    LOAD_DAY_NAME STRING,  -- Naya Column
    LOAD_QUARTER INT,
    -- Metadata
    RECORD_HASH STRING, 
    EFF_START_DT TIMESTAMP_NTZ,
    EFF_END_DT TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
);

-- Snowpipe
CREATE OR REPLACE PIPE customer_pipe 
AUTO_INGEST = TRUE 
AS
COPY INTO customer_raw (raw_content, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @my_s3_stage)
FILE_FORMAT = (FORMAT_NAME = 'my_json_format');

desc pipe customer_pipe ;

-- ==========================================
-- 3. SILVER TASK (Extraction Logic Updated)
-- ==========================================
CREATE OR REPLACE TASK SNOW_PROJECT.PIPE_SCHEMA.PROCESS_SCD2_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_STREAM')
AS
MERGE INTO SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL AS target
USING (
    SELECT sub.* FROM (
        SELECT 
            raw_content:CUSTOMER_ID::STRING as cid,
            raw_content:CUSTOMER_NAME::STRING as cname,
            raw_content:EMAIL_ID::STRING as email,
            raw_content:PHONE_NUMBER::STRING as phone,
            raw_content:CITY::STRING as city,
            raw_content:STATE::STRING as state,
            raw_content:COUNTRY::STRING as country,
            raw_content:CUSTOMER_SEGMENT::STRING as segment,
            raw_content:CUSTOMER_STATUS::STRING as status,
            raw_content:CREDIT_RATING::STRING as rating,
            raw_content:SOURCE_SYSTEM::STRING as source,
            file_name as fname,
            -- Date Extraction logic
            YEAR(raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ) as l_year,
            MONTH(raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ) as l_month,
            DAY(raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ) as l_day,
            DAYNAME(raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ) as l_day_name, -- Day Name (Mon, Tue, etc.)
            QUARTER(raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ) as l_quarter,
            HASH(raw_content:CUSTOMER_NAME, raw_content:EMAIL_ID, raw_content:PHONE_NUMBER, 
                 raw_content:CITY, raw_content:STATE, raw_content:COUNTRY, raw_content:CUSTOMER_SEGMENT)::STRING as current_row_hash,
            raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ as load_ts,
            NULL AS merge_key 
        FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_STREAM s
        WHERE NOT EXISTS (
            SELECT 1 FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL f 
            WHERE f.CUSTOMER_ID = s.raw_content:CUSTOMER_ID::STRING 
            AND f.RECORD_HASH = HASH(s.raw_content:CUSTOMER_NAME, s.raw_content:EMAIL_ID, s.raw_content:PHONE_NUMBER, 
                                     s.raw_content:CITY, s.raw_content:STATE, s.raw_content:COUNTRY, s.raw_content:CUSTOMER_SEGMENT)::STRING
            AND f.IS_CURRENT = TRUE
        )
        UNION ALL
        SELECT 
            raw_content:CUSTOMER_ID::STRING as cid,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, NULL, -- Extra NULL for Day Name
            NULL as current_row_hash,
            raw_content:LOAD_TIMESTAMP::TIMESTAMP_NTZ as load_ts,
            raw_content:CUSTOMER_ID::STRING AS merge_key
        FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_STREAM s
        WHERE EXISTS (
            SELECT 1 FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL f 
            WHERE f.CUSTOMER_ID = s.raw_content:CUSTOMER_ID::STRING 
            AND f.RECORD_HASH <> HASH(s.raw_content:CUSTOMER_NAME, s.raw_content:EMAIL_ID, s.raw_content:PHONE_NUMBER, 
                                      s.raw_content:CITY, s.raw_content:STATE, s.raw_content:COUNTRY, s.raw_content:CUSTOMER_SEGMENT)::STRING
            AND f.IS_CURRENT = TRUE
        )
    ) sub
) AS src
ON target.CUSTOMER_ID = src.merge_key AND target.IS_CURRENT = TRUE
WHEN MATCHED THEN
    UPDATE SET target.EFF_END_DT = src.load_ts, target.IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ID, PHONE_NUMBER, CITY, STATE, COUNTRY, 
            CUSTOMER_SEGMENT, CUSTOMER_STATUS, CREDIT_RATING, SOURCE_SYSTEM, FILE_NAME, 
            LOAD_YEAR, LOAD_MONTH, LOAD_DAY, LOAD_DAY_NAME, LOAD_QUARTER, RECORD_HASH, 
            EFF_START_DT, EFF_END_DT, IS_CURRENT)
    VALUES (src.cid, src.cname, src.email, src.phone, src.city, src.state, src.country, 
            src.segment, src.status, src.rating, src.source, src.fname, 
            src.l_year, src.l_month, src.l_day, src.l_day_name, src.l_quarter, src.current_row_hash, 
            src.load_ts, '9999-12-31'::TIMESTAMP_NTZ, TRUE);

-- ==========================================
-- 4. GOLD LAYER (New Columns included)
-- ==========================================
CREATE OR REPLACE SCHEMA GOLD_SCHEMA;

CREATE OR REPLACE TABLE GOLD_SCHEMA.DIM_CUSTOMER_GOLD (
    CUSTOMER_ID STRING,
    CUSTOMER_NAME STRING,
    EMAIL_ID STRING,
    CITY STRING,
    LOAD_YEAR INT,
    LOAD_MONTH INT,
    LOAD_DAY INT,
    LOAD_DAY_NAME STRING, -- Added here
    LOAD_QUARTER INT,
    SOURCE_SYSTEM STRING,
    FILE_NAME STRING,
    GOLD_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE GOLD_SCHEMA.FACT_CUSTOMER_METRICS_GOLD (
    CUSTOMER_ID STRING,
    CREDIT_RATING STRING,
    RATING_SCORE INT,
    LOAD_YEAR INT, -- Fact table mein bhi year hona accha hota h trend ke liye
    RECORD_START_DATE TIMESTAMP_NTZ
);


-- 3rd Table: All Details (Dimension + Fact Metrics merged)
CREATE OR REPLACE TABLE GOLD_SCHEMA.DM_CUSTOMER_FULL_DETAILS (
    CUSTOMER_ID STRING,
    CUSTOMER_NAME STRING,
    EMAIL_ID STRING,
    PHONE_NUMBER STRING,
    CITY STRING,
    STATE STRING,
    COUNTRY STRING,
    CUSTOMER_SEGMENT STRING,
    CUSTOMER_STATUS STRING,
    CREDIT_RATING STRING,
    RATING_SCORE INT,          -- Fact column
    SOURCE_SYSTEM STRING,
    FILE_NAME STRING,
    LOAD_YEAR INT,
    LOAD_MONTH INT,
    LOAD_DAY INT,
    LOAD_DAY_NAME STRING,
    LOAD_QUARTER INT,
    GOLD_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);




-- ==========================================
-- 5. GOLD TASK (Dependency Chain)
-- ==========================================
CREATE OR REPLACE TASK SNOW_PROJECT.PIPE_SCHEMA.LOAD_GOLD_TABLES_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER SNOW_PROJECT.PIPE_SCHEMA.PROCESS_SCD2_TASK
AS
BEGIN
    -- 1. Dimension Table Update
    INSERT OVERWRITE INTO GOLD_SCHEMA.DIM_CUSTOMER_GOLD
    SELECT 
        CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ID, CITY, 
        LOAD_YEAR, LOAD_MONTH, LOAD_DAY, LOAD_DAY_NAME, LOAD_QUARTER, 
        SOURCE_SYSTEM, FILE_NAME, CURRENT_TIMESTAMP()
    FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL
    WHERE IS_CURRENT = TRUE;

    -- 2. Fact Table Update
    INSERT OVERWRITE INTO GOLD_SCHEMA.FACT_CUSTOMER_METRICS_GOLD
    SELECT 
        CUSTOMER_ID, CREDIT_RATING,
        CASE 
            WHEN CREDIT_RATING = 'A' THEN 100
            WHEN CREDIT_RATING = 'B' THEN 80
            WHEN CREDIT_RATING = 'C' THEN 60
            ELSE 40 
        END AS RATING_SCORE,
        LOAD_YEAR,
        EFF_START_DT
    FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL
    WHERE IS_CURRENT = TRUE;

    -- 3. Nayi Full Details Table Update (Sari details ek sath)
    INSERT OVERWRITE INTO GOLD_SCHEMA.DM_CUSTOMER_FULL_DETAILS
    SELECT 
        CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ID, PHONE_NUMBER, 
        CITY, STATE, COUNTRY, CUSTOMER_SEGMENT, 
        CUSTOMER_STATUS, CREDIT_RATING,
        CASE 
            WHEN CREDIT_RATING = 'A' THEN 100
            WHEN CREDIT_RATING = 'B' THEN 80
            WHEN CREDIT_RATING = 'C' THEN 60
            ELSE 40 
        END AS RATING_SCORE,
        SOURCE_SYSTEM, FILE_NAME,
        LOAD_YEAR, LOAD_MONTH, LOAD_DAY, LOAD_DAY_NAME, LOAD_QUARTER,
        CURRENT_TIMESTAMP()
    FROM SNOW_PROJECT.PIPE_SCHEMA.CUSTOMER_FINAL
    WHERE IS_CURRENT = TRUE;
END;

-- ==========================================
-- 5. START PIPELINE (Sahi Order)
-- ==========================================
-- 1. Pehle child ko resume karein
ALTER TASK SNOW_PROJECT.PIPE_SCHEMA.LOAD_GOLD_TABLES_TASK RESUME;

-- Phir parent task ko resume karein
ALTER TASK SNOW_PROJECT.PIPE_SCHEMA.PROCESS_SCD2_TASK RESUME;

-- NOTE: Niche 'suspend' mat likhna warna task band ho jayenge.
SHOW TASKS IN SCHEMA SNOW_PROJECT.PIPE_SCHEMA;

-- Verification
SELECT * FROM GOLD_SCHEMA.DIM_CUSTOMER_GOLD;

select * from GOLD_SCHEMA.FACT_CUSTOMER_METRICS_GOLD;  -- fact table(numeric data only)
SELECT * FROM GOLD_SCHEMA.DIM_CUSTOMER_GOLD;  -- dimensional table(descriptive data only)
SELECT * FROM GOLD_SCHEMA.DM_CUSTOMER_FULL_DETAILS; -- fact and dimensional table data(all data is here)