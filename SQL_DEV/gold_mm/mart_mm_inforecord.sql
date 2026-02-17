WITH silver_ziinforecorgdata_cte AS (
    SELECT
        purchasinginforecord,
        purchasingorganization,
        purchasinginforecordcategory,
        plant,
        materialplanneddeliverydurn,
        pricevalidityenddate,
        ingestiontime,
        isupsert,
        isdelete,
        isinsert,
        changetype
    FROM silver_mm_ziinforecorgdata
    WHERE isdelete = false
),

silver_zimmpurgdocitem_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        purchasingdocumentcategory,
        material,
        material_description,
        quantity_numerator,
        quantity_denominator,
        plant,
        materialgroup,
        materialgroupname,
        orderquantityunit,
        baseunit,
        free_item,
        purchaserequisition,
        purchaserequisitionitem,
        purchasinginforecord,
        orderquantity,
        taxcode,
        safetystockquantity,
        netamount,
        netpricequantity,
        netpriceamount
    FROM silver_mm_zimmpurgdocitem
    WHERE isdelete = false
      AND purchasingdocumentdeletioncode IS NULL
),

silver_zmmpurchasingdoc_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentcategory,
        purchasingdocumenttype,
        releasecode,
        supplier,
        suppliername,
        purchasinggroup,
        documentcurrency,
        exchangerate,
        purchasingdocumentorderdate,
        paymentterms,
        incotermsclassification,
        purchasingdocumentcondition,
        purgreleasetimetotalamount
    FROM silver_mm_zmmpurchasingdoc
    WHERE isdelete = false
),

/* =========================================================
   INFORECORD JOIN WITH PLANT-FIRST + FALLBACK PLANT = NULL
   ========================================================= */
inforecord_ranked_cte AS (
    SELECT
        po.purchasingdocument,
        po.purchasingdocumentitem,

        i.purchasinginforecord,
        i.purchasingorganization,
        i.purchasinginforecordcategory,
        i.plant AS info_plant,
        i.materialplanneddeliverydurn,
        COALESCE(i.pricevalidityenddate, DATE '9999-12-31') AS pricevalidityenddate,
        i.ingestiontime,
        i.isinsert,
        i.isupsert,
        i.isdelete,
        i.changetype,

        ROW_NUMBER() OVER (
            PARTITION BY po.purchasingdocument, po.purchasingdocumentitem
            ORDER BY
                CASE
                    WHEN i.plant = po.plant THEN 1   -- match plant
                    WHEN i.plant IS NULL THEN 2      -- fallback
                    ELSE 3
                END
        ) AS rn

    FROM silver_zimmpurgdocitem_cte po
    LEFT JOIN silver_ziinforecorgdata_cte i
        ON i.purchasinginforecord = po.purchasinginforecord
)

SELECT
    /* ===== INFO RECORD ===== */
    i.purchasinginforecord,
    i.purchasingorganization,
    i.purchasinginforecordcategory,
    i.info_plant AS plant,
    i.materialplanneddeliverydurn,
    i.pricevalidityenddate,

    /* ===== PO ITEM (GRAIN) ===== */
    po.purchasingdocument,
    po.purchasingdocumentitem,
    po.material,
    po.material_description,
    po.materialgroup,
    po.materialgroupname,
    po.baseunit,
    po.orderquantityunit,
    po.free_item,
    po.purchaserequisition,
    po.purchaserequisitionitem,
    po.orderquantity,
    po.netpriceamount,
    po.netpricequantity,
    po.netamount,
    po.taxcode,
    po.safetystockquantity,

    /* ===== PO HEADER ===== */
    h.supplier,
    h.suppliername,
    h.releasecode,
    h.purchasinggroup,
    h.documentcurrency,
    h.exchangerate,
    h.purchasingdocumentorderdate,
    h.paymentterms,
    h.incotermsclassification,
    h.purchasingdocumentcondition,
    h.purgreleasetimetotalamount,

    /* ===== META ===== */
    i.ingestiontime,
    i.isinsert,
    i.isupsert,
    i.isdelete,
    i.changetype,
    current_timestamp() AS last_main_silver_modified_dt

FROM silver_zimmpurgdocitem_cte po

LEFT JOIN inforecord_ranked_cte i
    ON po.purchasingdocument = i.purchasingdocument
   AND po.purchasingdocumentitem = i.purchasingdocumentitem
   AND i.rn = 1

LEFT JOIN silver_zmmpurchasingdoc_cte h
    ON po.purchasingdocument = h.purchasingdocument 
