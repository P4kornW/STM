WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument
            ORDER BY ingestiontime DESC
        ) as rn
    FROM zmmpurchasingdoc
    WHERE 
        purchasingdocument IS NOT NULL
)

SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentcategory AS STRING) AS purchasingdocumentcategory,  
    CAST(R.purchasingdocumenttype AS STRING) AS purchasingdocumenttype,
    CAST(R.releasecode AS STRING) AS releasecode,
    CAST(R.supplier AS STRING) AS supplier,
    CAST(S.suppliername AS STRING) AS suppliername,
    CAST(R.purchasinggroup AS STRING) AS purchasinggroup,
    CAST(G.purchasinggroupname as STRING) AS purchasinggroupname,
    CAST(R.documentcurrency AS STRING) AS documentcurrency,
    CAST(R.exchangerate AS DECIMAL(18,4)) AS exchangerate,
    to_date(R.purchasingdocumentorderdate, 'yyyyMMdd') AS purchasingdocumentorderdate,
    CAST(R.paymentterms AS STRING) AS paymentterms,
    CAST(R.incotermsclassification AS STRING) AS incotermsclassification,  
    CAST(R.purchasingdocumentcondition AS STRING) AS purchasingdocumentcondition,
    CAST(R.purgreleasetimetotalamount AS DECIMAL(18,2)) AS purgreleasetimetotalamount,
    to_date(R.creationdate,'yyyyMMdd') AS creationdate,
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