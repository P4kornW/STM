WITH 

/* =========================
   1) หา scope ของ PO item ที่ต้อง reprocess
   ========================= */
Final_Processing_Scope AS (
    SELECT DISTINCT R.purchasingdocument, R.purchasingdocumentitem
    FROM zimmpurgdocitem R
    LEFT JOIN ziproduct PR
        ON R.material = PR.product
    LEFT JOIN ziprduom U
        ON PR.product = U.product
    LEFT JOIN ziprdplant P
        ON R.material = P.product
       AND R.plant = P.plant
    LEFT JOIN ziproductgrp PGR
        ON R.materialgroup = PGR.materialgroup
    WHERE 
        -- transaction เปลี่ยนใน 1 วันล่าสุด
        R.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)

        -- หรือ master ตัวใดตัวหนึ่งเปลี่ยนใน 1 วันล่าสุด
        OR PR.ingestiontime  >= (current_timestamp() - INTERVAL 1 DAY)
        OR U.ingestiontime   >= (current_timestamp() - INTERVAL 1 DAY)
        OR P.ingestiontime   >= (current_timestamp() - INTERVAL 1 DAY)
        OR PGR.ingestiontime >= (current_timestamp() - INTERVAL 1 DAY)
),

/* =========================
   2) ดึงข้อมูลเต็มของ key ที่อยู่ใน scope
   ========================= */
Ranked_Raw_Batch AS (
    SELECT 
        R.*, 
        ROW_NUMBER() OVER (
            PARTITION BY R.purchasingdocument, R.purchasingdocumentitem
            ORDER BY R.ingestiontime DESC
        ) AS rn
    FROM zimmpurgdocitem R
    INNER JOIN Final_Processing_Scope S
        ON  R.purchasingdocument     = S.purchasingdocument
        AND R.purchasingdocumentitem = S.purchasingdocumentitem
    WHERE 
        R.purchasingdocument IS NOT NULL 
        AND R.purchasingdocumentitem IS NOT NULL
)

/* =========================
   3) SELECT เหมือนของเดิม
   ========================= */
SELECT DISTINCT
    CAST(R.purchasingdocument AS STRING) AS purchasingdocument,
    CAST(R.purchasingdocumentitem AS STRING) AS purchasingdocumentitem,
    CAST(R.purchasingdocumentcategory AS STRING) AS purchasingdocumentcategory,
    CAST(R.material AS STRING) AS material,
    CAST(R.purchasingdocumentitemtext AS STRING) AS material_description,

    CAST(U.quantitynumerator AS DECIMAL(18,3)) AS quantity_numerator,
    CAST(U.quantitydenominator AS DECIMAL(18,3)) AS quantity_denominator,

    CAST(U_KG.quantitynumerator AS DECIMAL(18,3)) AS quantity_numerator_kg,
    CAST(U_KG.quantitydenominator AS DECIMAL(18,3)) AS quantity_denominator_kg,

    CAST(U_EA.quantitynumerator AS DECIMAL(18,3)) AS quantity_numerator_ea,
    CAST(U_EA.quantitydenominator AS DECIMAL(18,3)) AS quantity_denominator_ea,

    CAST(R.plant AS STRING) AS plant,
    CAST(PR.productgroup AS STRING) AS materialgroup,
    CAST(PGR.materialgroupname AS STRING) AS materialgroupname,

    CAST(R.netpriceamount AS DECIMAL(18,3)) AS netpriceamount,
    CAST(R.orderquantityunit AS STRING) AS orderquantityunit,
    CAST(PR.baseunit AS STRING) AS baseunit,

    CASE 
        WHEN R.invoiceisexpected IS NULL OR R.invoiceisexpected = '' THEN 'X'
        ELSE NULL                                                               
    END AS free_item,

    CAST(R.purchaserequisition AS STRING) AS purchaserequisition,
    CAST(R.purchaserequisitionitem AS STRING) AS purchaserequisitionitem,
    CAST(R.purchasinginforecord AS STRING) AS purchasinginforecord,
    CAST(R.orderquantity AS DECIMAL(18,3)) AS orderquantity,
    CAST(R.taxcode AS STRING) AS taxcode,
    CAST(P.safetystockquantity AS DECIMAL(18,3)) as safetystockquantity,

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

LEFT JOIN ziprduom U_KG
    ON PR.product = U_KG.product
   AND U_KG.alternativeunit = 'KG'

LEFT JOIN ziprduom U_EA
    ON PR.product = U_EA.product
   AND U_EA.alternativeunit = 'EA'

LEFT JOIN ziprdplant P
    ON R.material = P.product
   AND R.plant = P.plant

LEFT JOIN ziproductgrp PGR
    ON R.materialgroup = PGR.materialgroup

WHERE rn = 1
