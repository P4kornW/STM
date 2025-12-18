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
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    to_date(R.approvedate, 'yyyyMMdd') AS approvedate,
    CAST(R.approvetime AS STRING) AS approvetime,
    TO_TIMESTAMP(
        CONCAT(R.approvedate, R.approvetime),
        'yyyyMMddHHmmss'
    ) AS approve_dt,
    CAST(R.approvercode AS STRING) AS approvercode,
    CAST(R.approveusername AS STRING) AS approveusername,
    CAST(R.approverdescription AS STRING) AS approverdescription,
    CAST(U.fullname AS STRING) AS approverfullname,
    CAST(R.isapprove AS STRING) AS isapprove,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() AS last_modified_dt
FROM Ranked_Raw_Batch R
LEFT JOIN ziuser U
    ON R.approveusername = U.userid
WHERE rn = 1
