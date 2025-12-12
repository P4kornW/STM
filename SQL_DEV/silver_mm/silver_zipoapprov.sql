WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zipoapprov
    WHERE
        purchasingdocument IS NOT NULL

)

SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    TO_TIMESTAMP(CONCAT(R.approvedate, R.approvetime),'yyyyMMddHHmmss') AS approve_dt,
    CAST(R.isapprove AS STRING) AS isapprove,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1