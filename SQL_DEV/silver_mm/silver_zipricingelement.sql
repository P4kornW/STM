WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY pricingdocument, pricingdocumentitem,pricingprocedurestep,pricingprocedurecounter
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zipricingelement
    WHERE ingestiontime >= (select coalesce(max(ingestiontime),'1900-01-01') - INTERVAL 6 HOUR from silver_mm_zipricingelement)
        AND pricingdocument IS NOT NULL 
        AND pricingdocumentitem IS NOT NULL
        AND pricingprocedurestep IS NOT NULL
        AND pricingprocedurecounter IS NOT NULL
)

SELECT DISTINCT
    CAST(R.pricingdocument AS STRING) AS pricingdocument,
    SUBSTR(CAST(R.pricingdocumentitem AS STRING), 2) AS pricingdocumentitem,
    CAST(R.pricingprocedurestep AS STRING) AS pricingprocedurestep,
    CAST(R.pricingprocedurecounter AS STRING) AS pricingprocedurecounter,
    CAST(R.conditiontype AS STRING) AS conditiontype,
    
    CASE
        WHEN R.conditiontype = 'ZFB3' THEN 'Freight'
        WHEN R.conditiontype = 'ZCB1' THEN 'Clearance'
        WHEN R.conditiontype = 'ZDB3' THEN 'Discount'
        ELSE null
        
    END AS condition_description,
    TO_TIMESTAMP(R.pricingdatetime,'yyyyMMddHHmmss') AS pricingdatetime,
    CAST(R.conditioncalculationtype AS STRING) AS conditioncalculationtype,
    CAST(R.conditionamount AS DECIMAL(18,3)) AS conditionamount,
    CAST(R.transactioncurrency AS STRING) AS transactioncurrency,
    CAST(R.conditioninactivereason AS STRING) AS conditioninactivereason,
    CAST(R.conditioncurrency AS STRING) AS conditioncurrency,
    CAST(R.conditionratevalue AS DECIMAL(18,3)) AS conditionratevalue,
    CAST(R.PriceDetnExchangeRate AS DECIMAL(18,4)) AS pricedetnexchangerate,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1