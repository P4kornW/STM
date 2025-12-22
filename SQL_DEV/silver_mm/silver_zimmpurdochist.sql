WITH Ranked_Raw_Batch AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument,
                         purchasingdocumentitem,
                         accountassignmentnumber,
                         purchasinghistorydocumenttype,
                         purchasinghistorydocumentyear,
                         purchasinghistorydocument,
                         purchasinghistorydocumentitem
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zimmpurdochist
    WHERE 
        purchasingdocument IS NOT NULL 
        AND purchasingdocumentitem IS NOT NULL
        AND accountassignmentnumber IS NOT NULL
        AND purchasinghistorydocumenttype IS NOT NULL
        AND purchasinghistorydocumentyear IS NOT NULL
        AND purchasinghistorydocument IS NOT NULL
        AND purchasinghistorydocumentitem IS NOT NULL
)

SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentitem AS STRING) AS purchasingdocumentitem,
    CAST(R.accountassignmentnumber AS STRING) AS accountassignmentnumber,
    CAST(R.purchasinghistorydocumenttype AS STRING) AS purchasinghistorydocumenttype,
    CAST(R.purchasinghistorydocumentyear AS STRING) AS purchasinghistorydocumentyear,
    CAST(R.purchasinghistorydocument AS STRING) AS purchasinghistorydocument,
    CAST(R.purchasinghistorydocumentitem AS STRING) AS purchasinghistorydocumentitem,
    CAST(R.goodsmovementtype AS STRING) AS goodsmovementtype,
    to_date(R.postingdate, 'yyyyMMdd') AS postingdate,
    CAST(R.quantity AS DECIMAL(18,3)) AS quantity,
    CAST(R.debitcreditcode AS STRING) AS debitcreditcode,
    CAST(R.purordamountincompanycodecrcy AS DECIMAL(18,2)) AS purordamountincompanycodecrcy,
    CAST(R.purchaseorderamount AS DECIMAL(18,2)) AS purchaseorderamount,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1