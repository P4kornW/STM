WITH 

/* =========================
   1) หา scope ของ PO ที่ต้อง reprocess
   ========================= */
Final_Processing_Scope AS (
    SELECT DISTINCT R.purchasingdocument
    FROM zipoapprov R
    LEFT JOIN ziuser U
        ON R.approveusername = U.userid
    WHERE
        -- transaction เปลี่ยน
        R.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)

        -- หรือ master user เปลี่ยน
        OR U.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)
),

/* =========================
   2) ดึงข้อมูลเต็มของ PO ที่อยู่ใน scope
   ========================= */
Ranked_Raw_Batch AS (
    SELECT
        R.*,
        ROW_NUMBER() OVER (
            PARTITION BY R.purchasingdocument
            ORDER BY 
                R.approvedate DESC,
                R.approvetime DESC,
                R.ingestiontime DESC
        ) AS rn
    FROM zipoapprov R
    INNER JOIN Final_Processing_Scope S
        ON R.purchasingdocument = S.purchasingdocument
    WHERE
        R.purchasingdocument IS NOT NULL

)

/* =========================
   3) SELECT final
   ========================= */
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
