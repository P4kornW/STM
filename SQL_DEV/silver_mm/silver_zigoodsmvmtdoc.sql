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

SELECT
    CAST(R.materialdocumentkey1       AS STRING)        AS materialdocumentkey1,
    CAST(R.materialdocumentkey2       AS STRING)        AS materialdocumentkey2,
    CAST(R.materialdocumentkey3       AS STRING)        AS materialdocumentkey3,
    CAST(R.materialdocumentkey4       AS STRING)        AS materialdocumentkey4,
    CAST(R.materialdocumentkey5       AS STRING)        AS materialdocumentkey5,
    CAST(R.materialdocumentkey6       AS STRING)        AS materialdocumentkey6,
    CAST(R.materialdocumentyear       AS STRING)        AS materialdocumentyear,
    CAST(R.materialdocument           AS STRING)        AS materialdocument,
    CAST(R.materialdocumentitem       AS STRING)        AS materialdocumentitem,
    CAST(R.plant                      AS STRING)        AS plant,
    CAST(R.storagelocation            AS STRING)        AS storagelocation,
    CAST(R.material                   AS STRING)        AS material,
    CAST(R.batch                      AS STRING)        AS batch,
    CAST(R.materialbaseunit           AS STRING)        AS materialbaseunit,
    CAST(R.entryunit                  AS STRING)        AS entryunit,
    TO_DATE(R.postingdate, 'yyyyMMdd')AS postingdate,
    CAST(R.purchaseorder              AS STRING)        AS purchaseorder,
    CAST(R.purchaseorderitem          AS STRING)        AS purchaseorderitem,
    CAST(R.manufacturingorder         AS STRING)        AS manufacturingorder,
    CAST(R.manufacturingorderitem     AS STRING)        AS manufacturingorderitem,
    CAST(R.createdbyuser              AS STRING)        AS createdbyuser,
    CAST(R.supplier                   AS STRING)        AS supplier,
    CAST(R.customer                   AS STRING)        AS customer,
    CAST(R.referencedocument          AS STRING)        AS referencedocument,
    CAST(R.goodsmovementtype          AS STRING)        AS goodsmovementtype,
    CAST(R.materialdocumentheadertext AS STRING)        AS materialdocumentheadertext,
    CAST(R.materialdocumentitemtext   AS STRING)        AS materialdocumentitemtext,
    CAST(R.debitcreditcode            AS STRING)        AS debitcreditcode,
    CAST(R.totalgoodsmvtamtincccrcy   AS DECIMAL(18,2)) AS totalgoodsmvtamtincccrcy,
    CAST(R.quantityinbaseunit         AS DECIMAL(18,2)) AS quantityinbaseunit,
    CAST(R.quantityinentryunit        AS DECIMAL(18,2)) AS quantityinentryunit,
    CAST(R.goodsreceiptqtyinorderunit AS DECIMAL(18,2)) AS goodsreceiptqtyinorderunit,

    
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1