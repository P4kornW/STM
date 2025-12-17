WITH Ranked_Raw_Batch AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument
            ORDER BY approvedate DESC, approvetime DESC, ingestiontime DESC
        ) AS rn
    FROM zipoapprov
    WHERE
        purchasingdocument IS NOT NULL
        -- AND isdelete = false
        AND isapprove = 'Yes'
)

SELECT
    CAST(purchasingdocument AS STRING) AS purchasingdocument,
    TO_TIMESTAMP(
        CONCAT(approvedate, approvetime),
        'yyyyMMddHHmmss'
    ) AS approve_dt,
    approvercode,
    approveusername,
    approverdescription,
    CAST(isapprove AS STRING) AS isapprove,
    ingestiontime,
    isupsert,
    isdelete,
    isinsert,
    changetype,
    current_timestamp() AS last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1
