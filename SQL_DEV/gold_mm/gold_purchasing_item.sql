WITH silver_zimmpurgdocitem_cte AS (
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
        orderquantityunit,
        baseunit,
        free_item,
        purchaserequisition,
        purchaserequisitionitem,
        purchasinginforecord,
        orderquantity,
        taxcode,
        safetystockquantity,
        materialgroupname
    FROM silver_mm_zimmpurgdocitem
),

silver_zipritem_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        purchaserequisition,
        purchaserequisitionitem,
        purchasereqnitemuniqueid
    FROM silver_mm_zipritem
),

silver_zimmprgdocsl_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,
        scheduleline,
        schedulelinedeliverydate
    FROM silver_mm_zimmprgdocsl where scheduleline = '0001'
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
    FROM silver_mm_ziprapprov
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
        purchasingdocumentcondition
    FROM silver_mm_zmmpurchasingdoc
),

/* =======================
   PO HISTORY (SIGN + DECIMAL)
   ======================= */
silver_zimmpurdochist_cte AS (
    SELECT
        purchasingdocument,
        purchasingdocumentitem,

        MAX(postingdate) AS lastest_postingdate,

        CAST(SUM(
            CASE -- GR Quantity
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE WHEN debitcreditcode = 'H' THEN -quantity ELSE quantity END
                ELSE 0
            END
        ) AS DECIMAL(18,3)) AS gr_qty,

        CAST(SUM(
            CASE -- GR Value in THB
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE
                        WHEN debitcreditcode = 'H'
                            THEN -CAST(purordamountincompanycodecrcy AS DECIMAL(18,2))
                        ELSE CAST(purordamountincompanycodecrcy AS DECIMAL(18,2))
                    END
                ELSE CAST(0 AS DECIMAL(18,2))
            END
        ) AS DECIMAL(18,2)) AS gr_value_thb,

        CAST(SUM(
            CASE -- GR Value in PO Currency
                WHEN goodsmovementtype IN ('101','102') THEN
                    CASE WHEN debitcreditcode = 'H'
                        THEN -CAST(purchaseorderamount AS DECIMAL(18,2))
                        ELSE CAST(purchaseorderamount AS DECIMAL(18,2))
                    END
                ELSE CAST(0 AS DECIMAL(18,2))
            END
        ) AS DECIMAL(18,2)) AS gr_value_pocurrency

    FROM silver_mm_zimmpurdochist
    GROUP BY purchasingdocument, purchasingdocumentitem
),

/* =======================
   PRICING ELEMENT
   ======================= */
silver_zipricingelement_cte AS (
    SELECT
        pricingdocument,
        pricingdocumentitem,

        CAST(SUM(
            CASE WHEN conditiontype = 'ZCB1' -- Clearance
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS DECIMAL(18,2)) AS clearance_amount,

        CAST(SUM(
            CASE WHEN conditiontype = 'ZDB3' -- Discount
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS DECIMAL(18,2)) AS discount_amount,

        CAST(SUM(
            CASE WHEN conditiontype = 'ZFB3' -- Freight
                 AND conditioninactivereason IS NULL
                 THEN conditionamount ELSE 0 END
        ) AS DECIMAL(18,2)) AS freight_amount,

        CAST(SUM(
            CASE -- PO Actual Value
                WHEN conditioninactivereason IS NULL
                THEN conditionamount ELSE 0
            END
        ) AS DECIMAL(18,2)) AS po_actual_value

    FROM silver_mm_zipricingelement
    GROUP BY pricingdocument, pricingdocumentitem
)

, latest_material_price_cte AS ( -- Latest Material Price per Unit
    SELECT
        material,
        plant,
        purchasingdocument,
        purchasingdocumentitem,

        CAST(
            po_actual_value / NULLIF(orderquantity, 0)
        AS DECIMAL(18,4)) AS latest_material_price_per_unit,

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
            d.purchasingdocumentorderdate,
            p.po_actual_value
        FROM silver_zimmpurgdocitem_cte po
        JOIN silver_zmmpurchasingdoc_cte d
            ON po.purchasingdocument = d.purchasingdocument
        JOIN silver_zipricingelement_cte p
            ON d.purchasingdocumentcondition = p.pricingdocument
           AND po.purchasingdocumentitem = p.pricingdocumentitem
        WHERE
            p.po_actual_value IS NOT NULL
            AND po.orderquantity > 0
    ) x
)


SELECT
    /* ========= PO ITEM ========= */
    po.purchasingdocument,
    po.purchasingdocumentitem,
    po.purchasingdocumentcategory,
    po.material,
    po.material_description,
    po.plant,
    po.materialgroup,
    po.materialgroupname,
    po.orderquantityunit,
    po.baseunit,
    po.free_item,
    po.purchaserequisition,
    po.purchaserequisitionitem,
    po.orderquantity,
    po.taxcode,
    po.purchasinginforecord,
    

    /* ========= PO HEADER ========= */
    d.purchasingdocumenttype,
    d.releasecode,
    d.supplier,
    d.suppliername,
    d.purchasinggroup,
    d.documentcurrency,
    d.exchangerate,
    d.purchasingdocumentorderdate,
    d.incotermsclassification,
    d.paymentterms,
    d.purchasingdocumentcondition,

    /* ========= APPROVAL ========= */
    pr.purchasereqnitemuniqueid,
    a.approvercode,
    a.approverdescription,
    a.approveusername,
    a.approverfullname,
    a.approvedate as po_approvedate,
    h.lastest_postingdate as last_gr_postingdate,
    ar.updatedate as pr_approvedate,
    a.isapprove,

    /* ========= LATEST MATERIAL PRICE ========= */
    lm.latest_material_price_per_unit,
    lm.purchasingdocumentorderdate AS latest_price_po_date,

    /* ========= PRICING ========= */
    COALESCE(p.clearance_amount,0)     AS total_clearance_amount,
    COALESCE(p.freight_amount,0)       AS total_freight_amount,
    COALESCE(p.discount_amount,0)      AS total_discount_amount,
    COALESCE(p.po_actual_value,0)      AS total_po_actual_value,

    /* ========= GR HISTORY ========= */
    COALESCE(h.gr_qty,0)               AS total_gr_qty,
    COALESCE(h.gr_value_pocurrency,0)  AS total_gr_value_po_currency,
    COALESCE(h.gr_value_thb,0)         AS total_gr_value_thb,

    /* ========= Schedule Line ========= */
    sl.schedulelinedeliverydate as po_delivery_date,


    /* ========= CALCULATED (GOLD) ========= */

    CAST(
        CASE
            WHEN h.gr_qty IS NULL OR h.gr_qty = 0 THEN NULL
            ELSE h.gr_value_thb / h.gr_qty
        END
    AS DECIMAL(18,4)) AS gr_value_per_unit,

    CAST(
        po.orderquantity - COALESCE(h.gr_qty,0)
    AS DECIMAL(18,3)) AS gr_quantity_remain,

    CAST(
        CASE
            WHEN po.quantity_numerator IS NULL
              OR po.quantity_denominator IS NULL
              OR po.quantity_denominator = 0
            THEN NULL
            ELSE (po.orderquantity * po.quantity_numerator) / po.quantity_denominator
        END
    AS DECIMAL(18,3)) AS material_quantity_conversion,

    DATEDIFF( day, ar.updatedate, a.approvedate ) AS pr_to_po_approval_days,
    DATEDIFF(day,a.approvedate,h.lastest_postingdate) AS po_approval_to_lastest_gr_days,
    DATEDIFF( day,ar.updatedate,h.lastest_postingdate) AS pr_to_lastest_gr_days

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
