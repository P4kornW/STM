WITH silver_zimmpurgdocitem_cte AS (
    SELECT -- <-- PO ITEM
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
        safetystockquantity
        -- ingestiontime,
        -- isupsert,
        -- isdelete,
        -- isinsert,
        -- changetype
    FROM silver_mm_zimmpurgdocitem
--     WHERE isdelete = false
),

silver_zipritem_cte AS (
    SELECT -- <-- PR ITEM
        purchasingdocument,
        purchasingdocumentitem,
        purchaserequisition,
        purchaserequisitionitem
    FROM silver_mm_zipritem
),

-- silver_zigoodsmvmtdoc_cte AS (
--     SELECT
--         purchaseorder,
--         purchaseorderitem
--     FROM silver_mm_zigoodsmvmtdoc
-- ),

silver_ziinforecorgdata_cte AS (
    SELECT -- <-- Purcharsing info record
        purchasinginforecord,
        plant,
        MAX(materialplanneddeliverydurn) AS materialplanneddeliverydurn,
        MAX(pricevalidityenddate) AS pricevalidityenddate
    FROM silver_mm_ziinforecorgdata
    GROUP BY purchasinginforecord,plant
),

silver_zipoapprov_cte AS (
    SELECT -- <-- PO Approv
        purchasingdocument,
        approve_dt,
        isapprove,
        approvercode,
        approverdescription,
        approveusername
    FROM silver_mm_zipoapprov
),

silver_zmmpurchasingdoc_cte AS(
    SELECT -- <-- PO Header
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

silver_zimmpurdochist_cte AS(
    SELECT -- <-- PO History
        purchasingdocument,
        purchasingdocumentitem,

        
        SUM(
            CASE
                WHEN goodsmovementtype = '101'
                THEN quantity
                ELSE 0
            END
        ) AS gr_qty,

        SUM(
            CASE
                WHEN goodsmovementtype = '101'
                THEN purordamountincompanycodecrcy
                ELSE 0
            END
        ) AS gr_value_thb,

        SUM(
            CASE
                WHEN goodsmovementtype = '101'
                THEN purchaseorderamount
                ELSE 0
            END
        ) AS gr_value_pocurrency

    
    FROM silver_mm_zimmpurdochist
    GROUP BY
        purchasingdocument,
        purchasingdocumentitem
),

silver_zipricingelement_cte AS (
    SELECT -- <-- Pricing
        pricingdocument,
        pricingdocumentitem,

        -- Clearance
        SUM(
            CASE WHEN conditiontype = 'ZCB1' 
                 AND (conditioninactivereason IS NULL)
                 THEN conditionamount
                 ELSE 0
            END
        ) AS clearance_amount,

        -- Discount
        SUM(
            CASE WHEN conditiontype = 'ZDB3'
                 AND (conditioninactivereason IS NULL)
                 THEN conditionamount
                 ELSE 0
            END
        ) AS discount_amount,

        -- Freight
        SUM(
            CASE WHEN conditiontype = 'ZFB3'
                 AND (conditioninactivereason IS NULL)
                 THEN conditionamount
                 ELSE 0
            END
        ) AS freight_amount,

        SUM(
            CASE
                WHEN conditioninactivereason IS NULL
                THEN conditionamount
                ELSE 0
            END
        ) AS po_actual_value

    FROM silver_mm_zipricingelement
    -- WHERE isdelete = false
    GROUP BY
        pricingdocument,
        pricingdocumentitem
)


SELECT
    -- -- PO Item
    po.purchasingdocument,
    po.purchasingdocumentitem,
    po.purchasingdocumentcategory,
    po.material,
    po.material_description,
    po.quantity_numerator,
    po.quantity_denominator,
    po.plant,
    po.materialgroup,
    po.orderquantityunit,
    po.free_item,
    po.purchaserequisition,
    po.purchaserequisitionitem,
    po.purchasinginforecord,
    po.orderquantity,
    po.taxcode,
    po.safetystockquantity,

    -- -- PO Header
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

    -- -- InforecordORG
    i.materialplanneddeliverydurn,
    i.pricevalidityenddate,

    -- -- PO Approve
    a.approvercode,
    a.approverdescription,
    a.approveusername,
    a.approve_dt,
    a.isapprove,

    -- -- Pricing
    COALESCE(p.clearance_amount,0) as total_clearance_amount,
    COALESCE(p.freight_amount,0) as total_freight_amount,
    COALESCE(p.discount_amount,0) as total_discount_amount,
    COALESCE(p.po_actual_value,0) as total_po_actual_value,

    -- -- Po history
    COALESCE(h.gr_qty,0) as total_gr_qty,
    COALESCE(h.gr_value_pocurrency,0) as total_gr_value_po_currency,
    COALESCE(h.gr_value_thb,0) as total_gr_value_thb,
    

    -- Calculation
    h.gr_value_thb / NULLIF(h.gr_qty,0) AS gr_value_per_unit,
    po.orderquantity - COALESCE(h.gr_qty,0) as gr_quantity_remain,
    (po.orderquantity * po.quantity_numerator) / po.quantity_denominator as material_quantity_conversion,
    po.baseunit


    -- -- PR reference
    -- pr.purchaserequisition,
    -- pr.purchaserequisitionitem,

    -- current_timestamp() AS last_modified_dt

FROM silver_zimmpurgdocitem_cte po
LEFT JOIN silver_zipritem_cte pr
ON po.purchasingdocument = pr.purchasingdocument
AND po.purchasingdocumentitem = pr.purchasingdocumentitem
-- LEFT JOIN silver_zigoodsmvmtdoc_cte m
-- ON po.purchasingdocument = m.purchaseorder
-- AND po.purchasingdocumentitem = m.purchaseorderitem
LEFT JOIN silver_ziinforecorgdata_cte i
ON po.purchasinginforecord = i.purchasinginforecord
AND po.plant = i.plant
LEFT JOIN silver_zmmpurchasingdoc_cte d
ON po.purchasingdocument = d.purchasingdocument
LEFT JOIN silver_mm_zipoapprov a
ON d.purchasingdocument = a.purchasingdocument
LEFT JOIN silver_zipricingelement_cte p
ON d.purchasingdocumentcondition = p.pricingdocument
AND po.purchasingdocumentitem = p.pricingdocumentitem
LEFT JOIN silver_zimmpurdochist_cte h
ON po.purchasingdocument = h.purchasingdocument
AND po.purchasingdocumentitem = h.purchasingdocumentitem