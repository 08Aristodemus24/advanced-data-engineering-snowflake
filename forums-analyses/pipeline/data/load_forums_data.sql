-- USE weather_source_llc_frostbyte;
-- USE weather_source_llc_frostbyte.onpoint_id;

-- -- forecast day view has 15034055 rows
-- SELECT 
--     COUNT(postal_code) 
-- FROM forecast_day;
SELECT CURRENT_ACCOUNT();
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();

USE ROLE ACCOUNTADMIN;

-- env variable will come from running
-- snow git execute 
-- @advanced_data_engineering_snowflake/
-- branches/main/module-1/hamburg_weather/
-- pipeline/data/load_tasty_bytes.sql -D "env='STAGING'"
-- --database=COURSE_REPO --schema=PUBLIC 
-- because of the -D argument specifying env
-- variables with the command
CREATE OR ALTER DATABASE {{env}}_forums_analyses_db;

CREATE OR ALTER SCHEMA {{env}}_forums_analyses_db.raw;

CREATE OR ALTER SCHEMA {{env}}_forums_analyses_db.staging;

CREATE OR ALTER SCHEMA {{env}}_forums_analyses_db.intermediate;

CREATE OR ALTER SCHEMA {{env}}_forums_analyses_db.marts;

CREATE OR REPLACE WAREHOUSE fa_wh_xs
    -- set this to scale warehouse vertically
    WAREHOUSE_SIZE = XSMALL
    WAREHOUSE_TYPE = STANDARD
    AUTO_SUSPEND = 60

    -- set this to scale warehouse horizontally
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1

    SCALING_POLICY = STANDARD
    -- auto stop after 60 seconds
    AUTO_SUSPEND = 60
    -- 
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- place miscellaneous objects in public schema
CREATE OR ALTER FILE FORMAT {{env}}_forums_analyses_db.public.parq_ff
    TYPE = PARQUET;

CREATE OR REPLACE STAGE {{env}}_forums_analyses_db.public.sa_ext_stage_integration
    STORAGE_INTEGRATION = forums_analyses_si
    URL = 's3://forums-analyses-bucket' -- Replace with your S3 bucket and folder path
    FILE_FORMAT = parq_ff;

CREATE CATALOG INTEGRATION IF NOT EXISTS {{env}}_forums_analyses_db.public.delta_catalog_integration
    CATALOG_SOURCE = OBJECT_STORE
    TABLE_FORMAT = DELTA
    ENABLED = TRUE;

CREATE OR REPLACE ICEBERG TABLE {{env}}_forums_analyses_db.raw.raw_reddit_posts_comments
    CATALOG = delta_catalog_integration
    EXTERNAL_VOLUME = forums_analyses_ext_vol
    BASE_LOCATION = 'raw_reddit_posts_comments'
    AUTO_REFRESH = TRUE;

CREATE OR REPLACE ICEBERG TABLE {{env}}_forums_analyses_db.raw.raw_reddit_posts
    CATALOG = delta_catalog_integration
    EXTERNAL_VOLUME = forums_analyses_ext_vol
    BASE_LOCATION = 'raw_reddit_posts'
    AUTO_REFRESH = TRUE;

CREATE OR REPLACE ICEBERG TABLE {{env}}_forums_analyses_db.raw.raw_youtube_videos_comments
    CATALOG = delta_catalog_integration
    EXTERNAL_VOLUME = forums_analyses_ext_vol
    BASE_LOCATION = 'raw_youtube_videos_comments'
    AUTO_REFRESH = TRUE;

CREATE OR REPLACE ICEBERG TABLE {{env}}_forums_analyses_db.raw.raw_youtube_videos
    CATALOG = delta_catalog_integration
    EXTERNAL_VOLUME = forums_analyses_ext_vol
    BASE_LOCATION = 'raw_youtube_videos'
    AUTO_REFRESH = TRUE;


USE WAREHOUSE fa_wh_xs;

/* staging */
-- raw reddit posts comments
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.staging.stg_reddit_posts_comments AS (
    WITH reddit_posts_comments AS (
        SELECT
            post_id,
            post_name AS post_id_full,
            level,
            comment_id,
            comment_name AS comment_id_full,
            comment_upvotes,
            comment_downvotes,
            comment_created_at,
            comment_edited_at,
            comment_author_name AS comment_author_username,
            comment_author_fullname AS comment_author_id_full,
            comment_parent_id AS comment_parent_id_full,
            comment_body,
            added_at
            -- Add more columns as needed
        FROM {{env}}_forums_analyses_db.raw.raw_reddit_posts_comments;
    )
    -- This Jinja macro tells dbt: only execute the WHERE clause after the first run.

    SELECT *
    FROM reddit_posts_comments
);

-- raw reddit posts
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.staging.stg_reddit_posts AS (
    WITH reddit_posts AS (
        SELECT
            post_title,
            post_score,
            post_id,
            post_name AS post_id_full,
            post_url,
            post_author_name AS post_author_username,
            post_author_fullname AS post_author_id_full,
            post_body,
            post_created_at,
            post_edited_at,
            added_at
        FROM {{env}}_forums_analyses_db.raw.raw_reddit_posts;
    )

    SELECT *
    FROM reddit_posts
);


/* intermediate objects */
-- reddit posts
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.intermediate.int_reddit_posts AS (
    WITH post_activity_w_prob AS (
        SELECT
            *,
            CAST(TO_CHAR(DATE(post_created_at), 'YYYYMMDD') AS INT) AS date_id,
            SNOWFLAKE.CORTEX.SENTIMENT(post_body) AS probability
        FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts
    )

    SELECT
        *,
        CASE 
            WHEN probability < 0 THEN 'NEG'
            WHEN probability > 0 THEN 'POS'
            ELSE 'NEU'
        END AS sentiment
    FROM post_activity_w_prob
);

-- reddit users
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.intermediate.int_reddit_users AS (
    -- reddit users view
    SELECT
        DISTINCT
        comment_author_username AS username,
        comment_author_id_full AS user_id
    FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts
    WHERE comment_author_id_full IS NOT NULL

    UNION BY NAME

    SELECT
        DISTINCT
        post_author_id_full AS user_id,
        post_author_username AS username
    FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts_comments
    WHERE post_author_id_full IS NOT NULL
);

-- reddit dates
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.intermediate.int_reddit_dates AS (
    -- Get unique dates from the comments staging table
    WITH unique_comment_dates AS (
        SELECT 
            DISTINCT
            DATE(comment_created_at) AS date_actual -- Extract only the DATE part
        FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts_comments -- Reference your staging comments table
        WHERE comment_created_at IS NOT NULL
    ),

    -- Union with unique dates from the posts staging table
    unique_post_dates AS (
        SELECT 
            DISTINCT
            DATE(post_created_at) AS date_actual
        FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts -- Reference your staging posts table
        WHERE post_created_at IS NOT NULL
    ),

    -- Combine all unique dates into one list
    all_unique_dates AS (
        SELECT date_actual 
        FROM unique_comment_dates
        
        UNION BY NAME

        SELECT date_actual 
        FROM unique_post_dates
    )

    -- Final output with core attributes derived from the date
    SELECT
        date_actual,
        
        -- Generate the surrogate key (e.g., YYYYMMDD)
        CAST(TO_CHAR(date_actual, 'YYYYMMDD') AS INT) AS date_id,

        -- Basic date attributes
        EXTRACT(YEAR FROM date_actual) AS calendar_year,
        EXTRACT(MONTH FROM date_actual) AS calendar_month,
        EXTRACT(DAY FROM date_actual) AS calendar_day,
        
        -- Day of week attributes
        DAYNAME(date_actual) AS day_of_week,
        CASE
            -- if 0 or 6 it means sunday and saturday respectively
            -- 0, 1, 2, 3, 4, 5, 6 represent sunday to monday
            WHEN DAYOFWEEK(date_actual) IN (0, 6) THEN TRUE ELSE FALSE 
        END AS is_weekend
        
    FROM all_unique_dates
    ORDER BY date_actual
);

-- reddit comments
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.intermediate.int_reddit_comments AS (
    WITH comment_activity_w_prob AS (
        SELECT
            *,
            CAST(TO_CHAR(DATE(comment_created_at), 'YYYYMMDD') AS INT) AS date_id,
            SNOWFLAKE.CORTEX.SENTIMENT(comment_body) AS probability
        FROM {{env}}_forums_analyses_db.staging.stg_reddit_posts_comments
    )

    SELECT
        *,
        CASE 
            WHEN probability < 0 THEN 'NEG'
            WHEN probability > 0 THEN 'POS'
            ELSE 'NEU'
        END AS sentiment
    FROM comment_activity_w_prob
);


/* marts objects */
-- reddit dates
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.marts.dim_reddit_dates AS (
    SELECT
        date_actual,
        date_id,
        calendar_year,
        calendar_month,
        calendar_day,
        day_of_week,
        is_weekend
    FROM {{env}}_forums_analyses_db.intermediate.int_reddit_dates
);

-- reddit users
CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.marts.dim_reddit_users AS (
    SELECT
        username,
        user_id
    FROM {{env}}_forums_analyses_db.intermediate.int_reddit_users
);

CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.marts.dim_reddit_posts AS (
    WITH reddit_dates AS (
        SELECT *
        FROM {{env}}_forums_analyses_db.intermediate.int_reddit_dates
    ),

    reddit_users AS (
        SELECT *
        FROM {{env}}_forums_analyses_db.intermediate.int_reddit_users
    )

    SELECT
        rp.post_title,
        rp.post_score,
        rp.post_id,
        rp.post_id_full,
        rp.post_url,
        -- post_author_username,
        
        rp.post_body,
        -- post_created_at,
        rp.post_edited_at,
        rp.added_at,
        rp.probability,
        rp.sentiment,

        rd.date_id,
        ru.user_id
    FROM {{env}}_forums_analyses_db.intermediate.int_reddit_posts rp
    LEFT JOIN reddit_dates rd
    ON rp.date_id = rd.date_id
    LEFT JOIN reddit_users ru
    ON rp.post_author_id_full = ru.user_id
);

CREATE OR REPLACE VIEW {{env}}_forums_analyses_db.marts.dim_reddit_comments AS (
    WITH reddit_posts AS (
        SELECT * 
        FROM {{env}}_forums_analyses_db.marts.dim_reddit_posts
    ),

    reddit_dates AS (
        SELECT *
        FROM {{env}}_forums_analyses_db.marts.dim_reddit_dates
    ),

    reddit_users AS (
        SELECT *
        FROM {{env}}_forums_analyses_db.marts.dim_reddit_users
    )

    -- post_id_full, comment_author_id_full, and date_id are
    -- the foreign keys to the primary keys of the dimension
    -- tables
    SELECT
        rc.level,
        rc.comment_id,
        rc.comment_id_full,
        rc.comment_upvotes,
        rc.comment_downvotes,
        rc.comment_edited_at,
        rc.comment_parent_id_full,
        rc.comment_body,
        rc.added_at,
        rc.probability,
        rc.sentiment,

        -- foreign keys that refer to our dim tables
        rd.date_id,
        ru.user_id,
        rp.post_id_full
    FROM {{env}}_forums_analyses_db.marts.int_reddit_comments rc
    LEFT JOIN reddit_dates rd
    ON rc.date_id = rd.date_id
    LEFT JOIN reddit_users ru
    ON rc.comment_author_id_full = ru.user_id
    LEFT JOIN reddit_posts rp
    ON rc.post_id_full = rp.post_id_full
);

