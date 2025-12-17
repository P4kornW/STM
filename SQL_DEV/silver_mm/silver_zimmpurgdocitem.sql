WITH Ranked_Raw_Batch AS (
    -- LEVEL 1: DEDUPLICATION (The "Filter")
    -- We select * from Bronze to get all columns, calculate Rank, and filter bad flags.
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY purchasingdocument, purchasingdocumentitem
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM zimmpurgdocitem
    WHERE 
        purchasingdocument IS NOT NULL 
        AND purchasingdocumentitem IS NOT NULL
)

SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentitem AS STRING) AS purchasingdocumentitem,
    CAST(R.purchasingdocumentcategory AS STRING) AS purchasingdocumentcategory,
    CAST(R.material AS STRING) AS material,
    CAST(R.purchasingdocumentitemtext AS STRING) AS material_description,
    CAST(U.quantitynumerator AS DOUBLE) AS quantity_numerator,
    CAST(U.quantitydenominator AS DOUBLE) AS quantity_denominator,
    CAST(R.plant AS STRING) AS plant,
    CAST(PR.productgroup AS STRING) AS materialgroup,
    CAST(R.orderquantityunit AS STRING) AS orderquantityunit,
    CAST(PR.baseunit AS STRING) AS baseunit,
    CAST(R.invoiceisexpected AS STRING) AS free_item,
    CAST(R.purchaserequisition AS STRING) AS purchaserequisition,
    CAST(R.purchaserequisitionitem AS STRING) AS purchaserequisitionitem,
    CAST(R.purchasinginforecord AS STRING) AS purchasinginforecord,
    CAST(R.orderquantity AS DOUBLE) AS orderquantity,
    CAST(R.taxcode AS STRING) AS taxcode,
    CAST(P.safetystockquantity AS DOUBLE) as safetystockquantity,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() as last_modified_dt
FROM Ranked_Raw_Batch R
LEFT JOIN ziproduct PR
    ON R.material = PR.product
LEFT JOIN ziprduom U
    ON PR.product = U.product
    AND R.orderquantityunit = U.alternativeunit
LEFT JOIN ziprdplant P
    ON R.material = P.product
    AND R.plant = P.plant
WHERE rn = 1