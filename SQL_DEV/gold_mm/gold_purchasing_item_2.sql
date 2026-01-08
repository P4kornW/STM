WITH silver_zimmpurgdocitem_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        purchasingdocumentcategory,
        material,
        material_description,
        quantity_numerator,
        quantity_denominator,
        quantity_numerator_kg,
        quantity_denominator_kg,
        quantity_numerator_ea,
        quantity_denominator_ea,
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
        netpriceamount,
        netamount,
        netpricequantity,
        ingestiontime,
        isupsert,
        isdelete,
        isinsert,
        changetype
    FROM silver_mm_zimmpurgdocitem WHERE isdelete = false
),

silver_zipritem_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        purchaserequisition,
        purchaserequisitionitem,
        purchasereqnitemuniqueid
    FROM silver_mm_zipritem WHERE isdelete = false
),

silver_zimmprgdocsl_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        scheduleline,
        schedulelinedeliverydate
    FROM silver_mm_zimmprgdocsl where scheduleline = '0001' and isdelete = false
),

silver_zipoapprov_cte AS (
    SELECT
        purchasingdocument,
        approvedate,
        approve_dt,
        isapprove,
        approvercode,
        approverdescription,
        approveusername,
        approverfullname
    FROM silver_mm_zipoapprov WHERE isdelete = false
),

silver_ziprapprov_cte AS (
    SELECT
        tabkey,
        purchasereqnitemuniqueid,
        updatedate,
        updatetime,
        update_dt
    FROM silver_mm_ziprapprov WHERE isdelete = false
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
        purchasinggroupname,
        documentcurrency,
        exchangerate,
        purchasingdocumentorderdate,
        paymentterms,
        incotermsclassification,
        purchasingdocumentcondition,
        purgreleasetimetotalamount
    FROM silver_mm_zmmpurchasingdoc WHERE isdelete = false
),

/* =======================
   PO HISTORY (SIGN + DECIMAL)
   ======================= */
silver_zimmpurdochist_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,

        MAX(postingdate) as latest_postingdate,

        SUM(
            CASE -- GR Quantity
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE WHEN debitcreditcode = 'H' THEN -quantity ELSE quantity END
                ELSE 0
            END ) AS gr_qty,

        SUM(
            CASE -- GR Value in THB
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE
                        WHEN debitcreditcode = 'H'
                            THEN -purordamountincompanycodecrcy 
                        ELSE purordamountincompanycodecrcy 
                    END
                ELSE 0 
            END
        ) AS gr_value_thb,

        SUM(
            CASE -- GR Value in PO Currency
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE WHEN debitcreditcode = 'H'
                        THEN -purchaseorderamount
                        ELSE purchaseorderamount
                    END
                ELSE 0
            END
        ) AS gr_value_pocurrency

    FROM silver_mm_zimmpurdochist WHERE isdelete = false
    GROUP BY purchasingdocument, purchasingdocumentitem 
),

/* =======================
   PRICING ELEMENT
   ======================= */
silver_zipricingelement_cte AS (
    SELECT
        pricingdocument,
        pricingdocumentitem,

        SUM(
            CASE WHEN conditiontype = 'ZCB1' -- Clearance
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS clearance_amount,

        SUM(
            CASE WHEN conditiontype = 'ZDB3' -- Discount
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS discount_amount,

        SUM(
            CASE WHEN conditiontype = 'ZFB3' -- Freight
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS freight_amount,

        SUM(
            CASE -- PO Actual Value
                WHEN conditioninactivereason IS NULL
                THEN conditionamount ELSE 0
            END
        ) AS po_actual_value

    FROM silver_mm_zipricingelement WHERE isdelete = false
    GROUP BY pricingdocument, pricingdocumentitem 
)

, latest_material_price_cte AS (
    SELECT
        material,
        plant,
        purchasingdocument,
        purchasingdocumentitem,

        CASE
            WHEN free_item = 'X' THEN NULL
            WHEN orderquantity = 0 THEN NULL
            ELSE CAST(netpriceamount / orderquantity AS DECIMAL(18,4))
        END AS latest_material_price_per_unit,

        purchasingdocumentorderdate,

        ROW_NUMBER() OVER (
            PARTITION BY material, plant
            ORDER BY purchasingdocumentorderdate DESC,
                     purchasingdocument DESC,
                     purchasingdocumentitem DESC
        ) AS rn
    FROM (
        SELECT
            po.material,
            po.plant,
            po.purchasingdocument,
            po.purchasingdocumentitem,
            po.orderquantity,
            po.netpriceamount,
            po.free_item,
            d.purchasingdocumentorderdate
        FROM silver_zimmpurgdocitem_cte po
        JOIN silver_zmmpurchasingdoc_cte d
            ON po.purchasingdocument = d.purchasingdocument
        WHERE po.netpriceamount IS NOT NULL AND po.netpriceamount <> 0
    ) x
)


SELECT

    po.purchasingdocument,
    po.purchasingdocumentitem,
    d.purchasingdocumentorderdate as purchasing_date,
    po.purchaserequisition as reference_pr_number,
    po.free_item,
    po.material,
    po.material_description,
    po.materialgroup,
    po.materialgroupname,
    d.purchasinggroup,
    d.purchasinggroupname,
    sl.schedulelinedeliverydate as po_delivery_date,
    po.netpriceamount,
    po.netamount,
    po.netpricequantity,
    h.latest_postingdate as latest_grdate,
    po.orderquantity as po_quantity,
    po.orderquantityunit as purchasing_unit,
    
    --- 1) convert ตาม UOM master ---

    CAST(
        CASE
            WHEN po.quantity_numerator IS NULL
                OR po.quantity_denominator IS NULL
                OR po.quantity_denominator = 0
            THEN NULL
            ELSE (po.orderquantity * po.quantity_numerator) / po.quantity_denominator
        END AS DECIMAL(18,3))
    AS material_qty_conversion,
    po.baseunit,
    
    --- 2) convert เป็น KG ถ้า RM ---
   CAST(
    CASE 
        WHEN LEFT(po.material,2) = 'RM' AND po.quantity_numerator_kg IS NOT NULL
            THEN (po.orderquantity * po.quantity_numerator_kg) / po.quantity_denominator_kg
            ELSE NULL
        END AS DECIMAL(18,3)
    ) AS quantity_in_kg,

    --- 3) convert เป็น EA ถ้า PK / SP ---
    CAST(
    CASE 
        WHEN LEFT(po.material,2) IN ('PK','SP') AND po.quantity_numerator_ea IS NOT NULL
            THEN (po.orderquantity * po.quantity_numerator_ea) / po.quantity_denominator_ea
            ELSE NULL
        END AS DECIMAL(18,3)
    ) AS quantity_in_ea,

    
    d.documentcurrency,
    d.exchangerate,

    COALESCE(h.gr_value_thb,0)         AS total_gr_value_thb,
    COALESCE(h.gr_value_pocurrency,0)  AS total_gr_value_po_currency,
    
    CAST(
        CASE
            WHEN h.gr_qty IS NULL OR h.gr_qty = 0 THEN NULL
            ELSE h.gr_value_thb / h.gr_qty
        END AS DECIMAL(18,5)) 
    AS gr_value_per_unit,
    

    COALESCE(p.freight_amount,0)       AS total_freight_amount,
    COALESCE(p.clearance_amount,0)     AS total_clearance_amount,
    COALESCE(h.gr_qty,0)               AS total_gr_qty,
    
    COALESCE(p.po_actual_value,0)      AS total_po_actual_value,
    substr(d.supplier,3) as supplier,
    d.suppliername,
    d.paymentterms,

    CASE
        WHEN po.orderquantity - COALESCE(h.gr_qty,0) < 0 THEN 0
        ELSE po.orderquantity - COALESCE(h.gr_qty,0)
    END AS gr_quantity_remain,

    d.releasecode,
    a.approvercode,
    a.approverdescription,
    a.approverfullname,
    a.isapprove,
    a.approvedate as po_approvedate,
    ar.updatedate as pr_approvedate,
    d.incotermsclassification,
    d.purgreleasetimetotalamount,
    po.taxcode,
    COALESCE(p.discount_amount,0)      AS total_discount_amount,
    DATEDIFF( day, ar.updatedate, a.approvedate ) AS pr_to_po_approval_days,
    DATEDIFF( day, a.approvedate, h.latest_postingdate ) AS po_approval_to_latest_gr_days,
    DATEDIFF( day, ar.updatedate, h.latest_postingdate ) AS pr_approval_to_latest_gr_days,

    po.safetystockquantity,
    lm.latest_material_price_per_unit,
    lm.purchasingdocumentorderdate AS latest_price_po_date,
    po.purchasinginforecord,
    po.ingestiontime,
    po.isinsert,
    po.isupsert,
    po.isdelete,
    po.changetype,
    current_timestamp() as last_main_silver_modified_dt

    /* ========= JOINS ========= */

FROM silver_zimmpurgdocitem_cte po
LEFT JOIN silver_zipritem_cte pr
    ON po.purchasingdocument = pr.purchasingdocument
   AND po.purchasingdocumentitem = pr.purchasingdocumentitem
LEFT JOIN silver_ziprapprov_cte ar
    ON pr.purchasereqnitemuniqueid = ar.purchasereqnitemuniqueid
LEFT JOIN silver_zmmpurchasingdoc_cte d
    ON po.purchasingdocument = d.purchasingdocument
LEFT JOIN silver_zipoapprov_cte a
    ON d.purchasingdocument = a.purchasingdocument
LEFT JOIN silver_zipricingelement_cte p
    ON d.purchasingdocumentcondition = p.pricingdocument
   AND po.purchasingdocumentitem = p.pricingdocumentitem
LEFT JOIN silver_zimmpurdochist_cte h
    ON po.purchasingdocument = h.purchasingdocument
   AND po.purchasingdocumentitem = h.purchasingdocumentitem
LEFT JOIN latest_material_price_cte lm
    ON po.material = lm.material
   AND po.plant = lm.plant
   AND lm.rn = 1
LEFT JOIN silver_zimmprgdocsl_cte sl
    ON po.purchasingdocument = sl.purchasingdocument
    AND po.purchasingdocumentitem = sl.purchasingdocumentitem