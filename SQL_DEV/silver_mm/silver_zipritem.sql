WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchaserequisition, purchaserequisitionitem
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zipritem
    WHERE 
        purchaserequisition IS NOT NULL 
        AND purchaserequisitionitem IS NOT NULL
)

SELECT DISTINCT
    CAST(R.purchaserequisition AS STRING) AS purchaserequisition,
    CAST(R.purchaserequisitionitem AS STRING) AS purchaserequisitionitem,
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentitem AS STRING) AS purchasingdocumentitem,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1