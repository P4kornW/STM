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
        isapprove
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

silver_zipricingelement_cte AS (
    SELECT -- <-- Pricing
        pricingdocument,
        pricingdocumentitem,

        -- Clearance
        SUM(
            CASE WHEN conditiontype = 'ZCB1' 
                 AND (conditioninactivereason IS NULL)
                 THEN COALESCE(conditionamount, 0)
                 ELSE 0
            END
        ) AS clearance_amount,

        -- Discount
        SUM(
            CASE WHEN conditiontype = 'ZDB3'
                 AND (conditioninactivereason IS NULL)
                 THEN COALESCE(conditionamount, 0)
                 ELSE 0
            END
        ) AS discount_amount,

        -- Freight
        SUM(
            CASE WHEN conditiontype = 'ZFB3'
                 AND (conditioninactivereason IS NULL)
                 THEN COALESCE(conditionamount, 0)
                 ELSE 0
            END
        ) AS freight_amount

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
    a.approve_dt,
    a.isapprove,

    -- -- Pricing
    p.clearance_amount,
    p.freight_amount,
    p.discount_amount

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