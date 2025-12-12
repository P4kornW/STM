WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument, purchasingdocumentitem,scheduleline
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zimmprgdocsl
    WHERE 
        purchasingdocument IS NOT NULL 
        AND purchasingdocumentitem IS NOT NULL
        AND scheduleline IS NOT NULL
)

SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentitem AS STRING) AS purchasingdocumentitem,
    CAST(R.scheduleline AS STRING) AS scheduleline,
    to_date(R.schedulelinedeliverydate, 'yyyyMMdd') AS schedulelinedeliverydate,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1