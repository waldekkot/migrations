CREATE OR REPLACE FUNCTION PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(
    HIERARCHY_PATH VARCHAR,
    LEVELS INT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE
        WHEN HIERARCHY_PATH IS NULL OR LEVELS IS NULL THEN NULL
        WHEN LEVELS = 0 THEN HIERARCHY_PATH
        ELSE
            -- Remove the specified number of levels from the end
            -- Example: '/1/2/3/' with LEVELS=1 returns '/1/2/'
            REGEXP_REPLACE(
                HIERARCHY_PATH,
                '(/[^/]+){' || LEVELS || '}/$',
                '/'
            )
    END
$$;

CREATE OR REPLACE SEQUENCE ADVENTUREWORKS2017.DBO.ERRORLOG_SEQ START = 1 INCREMENT = 1;

