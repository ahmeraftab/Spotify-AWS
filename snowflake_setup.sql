-- Step 1: Database and Schema
CREATE OR REPLACE DATABASE spotify_data;
CREATE OR REPLACE SCHEMA spotify_data.spotify_schema;

-- Step 2: JSON File Format
CREATE OR REPLACE FILE FORMAT spotify_data.spotify_schema.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Step 3: External S3 Stage
CREATE OR REPLACE STAGE spotify_data.spotify_schema.spotify_stage
    URL = 's3://spotify-transformed-data-aa/transformed_data/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<your_aws_key_id>'
        AWS_SECRET_KEY = '<your_aws_secret_key>'
    )
    FILE_FORMAT = spotify_data.spotify_schema.json_format;

-- Step 4: Tracks Table
CREATE OR REPLACE TABLE spotify_data.spotify_schema.spotify_tracks (
    track_name   STRING,
    artist_name  STRING,
    album_name   STRING,
    popularity   INTEGER,
    duration_ms  INTEGER,
    release_date STRING,
    loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 5: Snowpipe — auto-ingest from S3
CREATE OR REPLACE PIPE spotify_data.spotify_schema.spotify_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO spotify_data.spotify_schema.spotify_tracks
    (track_name, artist_name, album_name, popularity, duration_ms, release_date)
FROM (
    SELECT
        $1:track_name::STRING,
        $1:artist_name::STRING,
        $1:album_name::STRING,
        $1:popularity::INTEGER,
        $1:duration_ms::INTEGER,
        $1:release_date::STRING
    FROM @spotify_data.spotify_schema.spotify_stage
)
FILE_FORMAT = (FORMAT_NAME = 'spotify_data.spotify_schema.json_format');

-- Step 6: Copy SQS ARN from here and add to S3 event notification
SHOW PIPES;

-- Step 7: Verify loaded data
SELECT * FROM spotify_data.spotify_schema.spotify_tracks
ORDER BY loaded_at DESC
LIMIT 20;