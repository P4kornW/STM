WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY materialdocumentkey1, materialdocumentkey2,materialdocumentkey3,materialdocumentkey4
            ,materialdocumentkey5,materialdocumentkey6
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zigoodsmvmtdoc
    WHERE 
        materialdocumentkey1 IS NOT NULL 
        AND materialdocumentkey2 IS NOT NULL
        AND materialdocumentkey3 IS NOT NULL
        AND materialdocumentkey4 IS NOT NULL
        AND materialdocumentkey5 IS NOT NULL
        AND materialdocumentkey6 IS NOT NULL
)

SELECT DISTINCT
    CAST(R.materialdocumentkey1 AS STRING) AS materialdocumentkey1,
    CAST(R.materialdocumentkey2 AS STRING) AS materialdocumentkey2,
    CAST(R.materialdocumentkey3 AS STRING) AS materialdocumentkey3,
    CAST(R.materialdocumentkey4 AS STRING) AS materialdocumentkey4,
    CAST(R.materialdocumentkey5 AS STRING) AS materialdocumentkey5,
    CAST(R.materialdocumentkey6 AS STRING) AS materialdocumentkey6,
    CAST(R.purchaseorder AS STRING) AS purchaseorder,
    CAST(R.purchaseorderitem AS STRING) AS purchaseorderitem,
    to_date(R.postingdate, 'yyyyMMdd') AS postingdate,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1