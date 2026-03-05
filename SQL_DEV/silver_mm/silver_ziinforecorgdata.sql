WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasinginforecord, purchasingorganization,purchasinginforecordcategory,plant
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM ziinforecorgdata
    WHERE ingestiontime >= (select coalesce(max(ingestiontime),'1900-01-01') - INTERVAL 6 HOUR from silver_mm_ziinforecorgdata)
        AND purchasinginforecord IS NOT NULL 
        AND purchasingorganization IS NOT NULL
        AND purchasinginforecordcategory IS NOT NULL
        
)

SELECT DISTINCT
    CAST(R.purchasinginforecord AS STRING) AS purchasinginforecord,
    CAST(R.purchasingorganization AS STRING) AS purchasingorganization,
    CAST(R.purchasinginforecordcategory AS STRING) AS purchasinginforecordcategory,
    CAST(R.plant AS STRING) AS plant,
    CAST(R.materialplanneddeliverydurn AS DOUBLE) AS materialplanneddeliverydurn,
    to_date(R.pricevalidityenddate, 'yyyyMMdd') AS pricevalidityenddate,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1