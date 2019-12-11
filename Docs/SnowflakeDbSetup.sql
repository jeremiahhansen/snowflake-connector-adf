-- This script will setup the necessary Snowflake objects to run the sample ADF
-- pipelines in this project.

-- ----------------------------------------------------------------------------
-- Script setup
-- ----------------------------------------------------------------------------
-- Step 1: Update the URL and AZURE_SAS_TOKEN properties for the ADF_BLOB_STAGE below

-- Step 2: Update the PASSWORD property for the ADF_DEMO_USER user below


-- ----------------------------------------------------------------------------
-- Create the database and related objects
-- ----------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE ADF_DEMO
    COMMENT = 'Database to demo the Snowflake Azure Data Factory (ADF) Connector (see https://github.com/jeremiahhansen/snowflake-connector-adf)';
USE DATABASE ADF_DEMO;

CREATE OR REPLACE SCHEMA TRIPPIN
    COMMENT = 'Schema to store data from the OData TripPin sample service (see https://www.odata.org/odata-services/)';

CREATE OR REPLACE SCHEMA LOGS
    COMMENT = 'Schema to store logs from the Snowflake Azure Data Factory (ADF) Connector';

CREATE OR REPLACE FILE FORMAT TRIPPIN.ADF_DATASET_FORMAT
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    RECORD_DELIMITER = '\n'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' -- double quotes
;

CREATE OR REPLACE STAGE TRIPPIN.ADF_BLOB_STAGE
    URL = 'azure://<storage account name>.blob.core.windows.net/blobstage'
    CREDENTIALS = (AZURE_SAS_TOKEN = '*****')
;
--LIST @ADF_DEMO.TRIPPIN.ADF_BLOB_STAGE;

CREATE OR REPLACE TABLE TRIPPIN.PEOPLE_STAGE
(
     UserName           VARCHAR         NOT NULL
    ,FirstName          VARCHAR         NOT NULL
    ,LastName           VARCHAR         NOT NULL
    ,MiddleName         VARCHAR         NULL
    ,Age                NUMBER          NULL
);

CREATE OR REPLACE TABLE TRIPPIN.PEOPLE
(
     UserName           VARCHAR         NOT NULL
    ,FirstName          VARCHAR         NOT NULL
    ,LastName           VARCHAR         NOT NULL
    ,MiddleName         VARCHAR         NULL
    ,Age                NUMBER          NULL
    ,META_CREATED_AT    TIMESTAMP_NTZ   NOT NULL
    ,META_UPDATED_AT    TIMESTAMP_NTZ   NOT NULL
);

CREATE OR REPLACE TABLE LOGS.ADF_PIPELINE_EXECUTION_LOG
(
     ADF_PIPELINE_RUN_ID        VARCHAR         NOT NULL
    ,ADF_PIPELINE_NAME          VARCHAR         NOT NULL
    ,ADF_PIPELINE_TRIGGER_ID    VARCHAR         NOT NULL
    ,ADF_PIPELINE_TRIGGER_NAME  VARCHAR         NOT NULL
    ,ADF_PIPELINE_TRIGGER_TYPE  VARCHAR         NOT NULL
    ,ADF_PIPELINE_TRIGGER_TIME  TIMESTAMP_NTZ   NOT NULL
    ,ADF_DATA_FACTORY_NAME      VARCHAR         NOT NULL
    ,EXECUTION_START_AT         TIMESTAMP_NTZ   NOT NULL
    ,EXECUTION_END_AT           TIMESTAMP_NTZ   NULL
    ,EXECUTION_STATUS           VARCHAR         NOT NULL
    ,ROWS_LOADED                NUMBER          NULL
    ,ROWS_INSERTED              NUMBER          NULL
    ,ROWS_UPDATED               NUMBER          NULL
    ,ROWS_DELETED               NUMBER          NULL
    ,META_CREATED_AT            TIMESTAMP_NTZ   NOT NULL
    ,META_UPDATED_AT            TIMESTAMP_NTZ   NOT NULL
);

CREATE OR REPLACE PROCEDURE TRIPPIN.SP_COPY_INTO_PEOPLE_STAGE(FILE_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE javascript
AS
$$
var validFileNameRegex = /^[\/\\A-Za-z0-9._-]+$/;
if (!validFileNameRegex.test(FILE_NAME)) {
  throw "Possible SQL Injection attack with file '" + FILE_NAME + "'";
}

var sqlCommand = "COPY INTO TRIPPIN.PEOPLE_STAGE \
  FROM @TRIPPIN.ADF_BLOB_STAGE \
  FILES = ('" + FILE_NAME + "') \
  FILE_FORMAT = (FORMAT_NAME = TRIPPIN.ADF_DATASET_FORMAT) \
  FORCE = TRUE";
var resultSet = snowflake.execute({sqlText: sqlCommand});

// Now get the previous DML result as a JSON object
sqlCommand = "SELECT OBJECT_CONSTRUCT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))";
resultSet = snowflake.execute({sqlText: sqlCommand});

// There should only be one row and one column returned
resultSet.next();
return resultSet.getColumnValue(1);
$$;


-- ----------------------------------------------------------------------------
-- Create the virtual warehouse
-- ----------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE ADF_DEMO_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
;


-- ----------------------------------------------------------------------------
-- Create the role, user and grant privileges
-- ----------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE ADF_DEMO_ROLE;
GRANT ROLE ADF_DEMO_ROLE TO ROLE SYSADMIN;
GRANT OWNERSHIP ON DATABASE ADF_DEMO TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON SCHEMA ADF_DEMO.TRIPPIN TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON SCHEMA ADF_DEMO.LOGS TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON FILE FORMAT ADF_DEMO.TRIPPIN.ADF_DATASET_FORMAT TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON STAGE ADF_DEMO.TRIPPIN.ADF_BLOB_STAGE TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON TABLE ADF_DEMO.TRIPPIN.PEOPLE_STAGE TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON TABLE ADF_DEMO.TRIPPIN.PEOPLE TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON TABLE ADF_DEMO.LOGS.ADF_PIPELINE_EXECUTION_LOG TO ROLE ADF_DEMO_ROLE;
GRANT OWNERSHIP ON PROCEDURE ADF_DEMO.TRIPPIN.SP_COPY_INTO_PEOPLE_STAGE(VARCHAR) TO ROLE ADF_DEMO_ROLE;
GRANT USAGE, MONITOR ON WAREHOUSE ADF_DEMO_WH TO ROLE ADF_DEMO_ROLE;
--SHOW GRANTS TO ROLE ADF_DEMO_ROLE;
--SHOW GRANTS TO ROLE SYSADMIN;

CREATE USER IF NOT EXISTS ADF_DEMO_USER
    PASSWORD = '*****'
    DEFAULT_ROLE = ADF_DEMO_ROLE
    DEFAULT_WAREHOUSE = 'ADF_DEMO_WH'
    DEFAULT_NAMESPACE = 'ADF_DEMO'
    MUST_CHANGE_PASSWORD = FALSE
;
GRANT ROLE ADF_DEMO_ROLE TO USER ADF_DEMO_USER;
--SHOW GRANTS TO USER ADF_DEMO_USER;
