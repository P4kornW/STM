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
        iscompletelydelivered,
        ingestiontime,
        isupsert,
        isdelete,
        isinsert,
        changetype
    FROM silver_mm_zimmpurgdocitem WHERE isdelete = false  AND purchasingdocumentdeletioncode IS NULL
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
    FROM silver_mm_zipoapprov 
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

        MAX(postingdate) as latest_grdate,

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
            CASE 
                WHEN conditiontype = 'ZCB1'
                 AND conditioninactivereason IS NULL
                THEN
                    CASE 
                        WHEN conditioncurrency = 'THB'
                            THEN conditionratevalue
                        ELSE
                            conditionamount * pricedetnexchangerate
                    END
                ELSE 0 
            END
        ) AS clearance_amount_thb,

        SUM(
            CASE WHEN conditiontype = 'ZDB3' -- Discount
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS discount_amount,

        SUM(
            CASE 
                WHEN conditiontype = 'ZDB3'
                 AND conditioninactivereason IS NULL
                THEN
                    CASE 
                        WHEN conditioncurrency = 'THB'
                            THEN conditionratevalue
                        ELSE
                            conditionamount * pricedetnexchangerate
                    END
                ELSE 0 
            END
        ) AS discount_amount_thb,

        SUM(
            CASE WHEN conditiontype = 'ZFB3' -- Freight
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS freight_amount,

        SUM(
            CASE 
                WHEN conditiontype = 'ZFB3'
                 AND conditioninactivereason IS NULL
                THEN
                    CASE 
                        WHEN conditioncurrency = 'THB'
                            THEN conditionratevalue
                        ELSE
                            conditionamount * pricedetnexchangerate
                    END
                ELSE 0 
            END
        ) AS freight_amount_thb,

        SUM(
            CASE -- PO Actual Value
                WHEN conditioninactivereason IS NULL
                THEN conditionamount ELSE 0
            END
        ) AS po_actual_value

    FROM silver_mm_zipricingelement WHERE isdelete = false
    GROUP BY pricingdocument, pricingdocumentitem 
)

, logic_cal_cte AS (
SELECT
    po.purchasingdocument,
    po.purchasingdocumentitem,

    /* ===== GR QTY REMAIN ===== */
    CASE
        WHEN po.iscompletelydelivered = 'X' THEN 0
        WHEN po.orderquantity - COALESCE(h.gr_qty,0) < 0 THEN 0
        ELSE po.orderquantity - COALESCE(h.gr_qty,0)
    END AS gr_quantity_remain,

    /* ===== PRICE PER UNIT (PO) ===== */
    CAST(
        CASE
            WHEN po.netpricequantity IS NULL OR po.netpricequantity = 0 THEN NULL
            ELSE po.netpriceamount / po.netpricequantity
        END AS DECIMAL(18,4)
    ) AS price_per_unit,

    /* ===== GR VALUE PER UNIT ===== */
    CAST(
        CASE
            WHEN h.gr_qty IS NULL OR h.gr_qty = 0 THEN NULL
        ELSE h.gr_value_thb / h.gr_qty
        END AS DECIMAL(18,4)
    ) AS gr_value_per_unit,

    /* ===== GR VALUE REMAIN ===== */
    CAST(
        CASE
            WHEN po.iscompletelydelivered = 'X' THEN 0
            ELSE
                (
                    CASE
                        WHEN po.orderquantity - COALESCE(h.gr_qty,0) < 0 THEN 0
                        ELSE po.orderquantity - COALESCE(h.gr_qty,0)
                    END
                )
                *
                (CASE
                    WHEN po.netpricequantity IS NULL OR po.netpricequantity = 0
                    THEN NULL
                    ELSE po.netpriceamount / po.netpricequantity
                 END)
        END AS DECIMAL(18,3)
    ) AS gr_value_remain,

    /* ===== GR VALUE REMAIN (THB) ===== */
    CAST(
        CASE
            WHEN po.iscompletelydelivered = 'X' THEN 0
            ELSE
                (
                    CASE
                        WHEN po.orderquantity - COALESCE(h.gr_qty,0) < 0 THEN 0
                        ELSE po.orderquantity - COALESCE(h.gr_qty,0)
                    END
                )
                *
                (CASE
                    WHEN po.netpricequantity IS NULL OR po.netpricequantity = 0
                    THEN NULL
                    ELSE po.netpriceamount / po.netpricequantity
                 END)
                * d.exchangerate
        END AS DECIMAL(18,4)
    ) AS gr_value_remain_thb

FROM silver_zimmpurgdocitem_cte po
LEFT JOIN silver_zimmpurdochist_cte h
    ON po.purchasingdocument = h.purchasingdocument
   AND po.purchasingdocumentitem = h.purchasingdocumentitem
LEFT JOIN silver_zmmpurchasingdoc_cte d
    ON po.purchasingdocument = d.purchasingdocument
)



, latest_material_price_cte AS (
    SELECT
        po.material,
        po.plant,
        lg.price_per_unit            AS latest_material_price_per_unit,
        d.purchasingdocumentorderdate AS latest_price_po_date,
        d.purchasingdocument AS latest_price_po_document,
        ROW_NUMBER() OVER (
            PARTITION BY po.material, po.plant
            ORDER BY d.purchasingdocumentorderdate DESC,
                     po.ingestiontime DESC
        ) AS rn
    FROM silver_zimmpurgdocitem_cte po
    LEFT JOIN logic_cal_cte lg
        ON po.purchasingdocument = lg.purchasingdocument
       AND po.purchasingdocumentitem = lg.purchasingdocumentitem
    LEFT JOIN silver_zmmpurchasingdoc_cte d
        ON po.purchasingdocument = d.purchasingdocument
    WHERE lg.price_per_unit IS NOT NULL
)



SELECT

    po.purchasingdocument,
    po.purchasingdocumentitem,
    d.purchasingdocumentorderdate as purchasing_date,
    po.purchaserequisition as reference_pr_number,
    d.purchasingdocumentcondition,
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
    CAST(po.netamount * d.exchangerate AS DECIMAL(18,4)) AS netamount_thb,
    po.netpricequantity,
    lg.price_per_unit,
    h.latest_grdate ,
    po.orderquantity as po_quantity,
    -- po.orderquantityunit as purchasing_unit,
    uom_p.unitofmeasure_e as purchasing_unit,
    
    --- 1) convert ตาม UOM master ---

    CAST(
    CASE
        -- มี conversion master PO → Base
        WHEN po.quantity_numerator IS NOT NULL
         AND po.quantity_denominator IS NOT NULL
         AND po.quantity_denominator <> 0
        THEN
            po.orderquantity
            * (po.quantity_numerator / po.quantity_denominator)

        -- ไม่มี conversion master
            ELSE NULL
        END
    AS DECIMAL(18,3)
    ) AS material_qty_conversion,
    -- po.baseunit,
    uom_b.unitofmeasure_e as baseunit,
    
    --- 2) convert เป็น KG ถ้า RM ---
   CAST(
    CASE
        -- มี conversion ไป KG
            WHEN po.quantity_numerator IS NOT NULL
            AND po.quantity_denominator IS NOT NULL
            AND po.quantity_denominator <> 0
            AND po.quantity_numerator_kg IS NOT NULL
            AND po.quantity_denominator_kg IS NOT NULL
            AND po.quantity_numerator_kg <> 0
            THEN
                po.orderquantity
                * (po.quantity_numerator / po.quantity_denominator)      -- PO → Base
                * (po.quantity_denominator_kg / po.quantity_numerator_kg) -- Base → KG

            -- PO unit เป็น G
            WHEN po.orderquantityunit = 'G'
            THEN po.orderquantity / 1000

            -- อื่นๆ
            ELSE null
        END
    AS DECIMAL(18,3)
    ) AS quantity_in_kg,

    --- 3) convert เป็น EA ถ้า PK / SP ---
    CAST(
    CASE
        -- มี conversion ไป EA
        WHEN po.quantity_numerator IS NOT NULL
         AND po.quantity_denominator IS NOT NULL
         AND po.quantity_denominator <> 0
         AND po.quantity_numerator_ea IS NOT NULL
         AND po.quantity_denominator_ea IS NOT NULL
         AND po.quantity_numerator_ea <> 0
        THEN
            po.orderquantity
            * (po.quantity_numerator / po.quantity_denominator)     -- PO → Base
            * (po.quantity_denominator_ea / po.quantity_numerator_ea) -- Base → EA

            -- ไม่มี conversion EA
            ELSE null
        END
    AS DECIMAL(18,3)
    ) AS quantity_in_ea,


    d.documentcurrency,
    d.exchangerate,

    h.gr_value_thb        AS total_gr_value_thb,
    h.gr_value_pocurrency  AS total_gr_value_po_currency,
    
    lg.gr_value_per_unit,
    

    CAST(p.freight_amount AS DECIMAL(18,4))      AS total_freight_amount,
    CAST(p.freight_amount_thb  AS DECIMAL(18,4))     AS total_freight_amount_thb,
    CAST(p.clearance_amount AS DECIMAL(18,4))     AS total_clearance_amount,
    CAST(p.clearance_amount_thb AS DECIMAL(18,4))    AS total_clearance_amount_thb,
    CAST(p.discount_amount   AS DECIMAL(18,4))   AS total_discount_amount,
    CAST(p.discount_amount_thb AS DECIMAL(18,4))  AS total_discount_amount_thb,
    h.gr_qty         AS total_gr_qty,
    
    p.po_actual_value     AS total_po_actual_value,
    CAST(p.po_actual_value * d.exchangerate AS DECIMAL(18,4)) AS total_po_actual_value_thb,
    substr(d.supplier,3) as supplier,
    d.suppliername,
    d.paymentterms,

    po.iscompletelydelivered,
    lg.gr_quantity_remain,
    lg.gr_value_remain,
    lg.gr_value_remain_thb,


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

    DATEDIFF( day, ar.updatedate, a.approvedate ) AS pr_to_po_approval_days,
    DATEDIFF( day, a.approvedate, h.latest_grdate ) AS po_approval_to_latest_gr_days,
    DATEDIFF( day, ar.updatedate, h.latest_grdate ) AS pr_approval_to_latest_gr_days,
    DATEDIFF( day, sl.schedulelinedeliverydate, h.latest_grdate ) AS po_delivery_date_to_latest_gr_days,

    po.safetystockquantity,
    lm.latest_material_price_per_unit,
    lm.latest_price_po_date,
    lm.latest_price_po_document,
    
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
    ON d.purchasingdocument = a.purchasingdocument AND d.releasecode = 'R'
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
LEFT JOIN logic_cal_cte lg
    ON po.purchasingdocument = lg.purchasingdocument
    AND po.purchasingdocumentitem = lg.purchasingdocumentitem
LEFT JOIN ziunitofmeasure uom_p
    ON po.orderquantityunit = uom_p.unitofmeasure
LEFT JOIN ziunitofmeasure uom_b
    ON po.baseunit = uom_b.unitofmeasure