WITH 

/* =========================
   1) หา scope ของ PO ที่ต้อง reprocess
   ========================= */
Final_Processing_Scope AS (
    SELECT DISTINCT R.purchasingdocument
    FROM zmmpurchasingdoc R
    LEFT JOIN zisupplier S
        ON R.supplier = S.supplier
    LEFT JOIN zimmpurchgroup G
        ON R.purchasinggroup = G.purchasinggroup
    WHERE
        -- transaction เปลี่ยน
        R.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)

        -- หรือ supplier master เปลี่ยน
        OR S.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)

        -- หรือ purchasing group master เปลี่ยน
        OR G.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)
),

/* =========================
   2) ดึงข้อมูลเต็มของ PO ที่อยู่ใน scope
   ========================= */
Ranked_Raw_Batch AS (
    SELECT 
        R.*, 
        ROW_NUMBER() OVER (
            PARTITION BY R.purchasingdocument
            ORDER BY R.ingestiontime DESC
        ) as rn
    FROM zmmpurchasingdoc R
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
    CAST(R.purchasingdocumentcategory AS STRING) AS purchasingdocumentcategory,  
    CAST(R.purchasingdocumenttype AS STRING) AS purchasingdocumenttype,
    CAST(R.releasecode AS STRING) AS releasecode,
    CAST(R.supplier AS STRING) AS supplier,
    CAST(S.suppliername AS STRING) AS suppliername,
    CAST(R.purchasinggroup AS STRING) AS purchasinggroup,
    CAST(G.purchasinggroupname AS STRING) AS purchasinggroupname,
    CAST(R.documentcurrency AS STRING) AS documentcurrency,
    CAST(R.exchangerate AS DECIMAL(18,4)) AS exchangerate,
    to_date(R.purchasingdocumentorderdate, 'yyyyMMdd') AS purchasingdocumentorderdate,
    CAST(R.paymentterms AS STRING) AS paymentterms,
    CAST(R.incotermsclassification AS STRING) AS incotermsclassification,  
    CAST(R.purchasingdocumentcondition AS STRING) AS purchasingdocumentcondition,
    CAST(R.purgreleasetimetotalamount AS DECIMAL(18,2)) AS purgreleasetimetotalamount,

    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt

FROM Ranked_Raw_Batch R
LEFT JOIN zisupplier S
    ON R.supplier = S.supplier
LEFT JOIN zimmpurchgroup G
    ON R.purchasinggroup = G.purchasinggroup
WHERE rn = 1
