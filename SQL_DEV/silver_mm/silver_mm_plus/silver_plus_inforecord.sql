 WITH silver_ziinforecorgdata_cte AS (
        SELECT
        purchasinginforecord,
        purchasingorganization,
        purchasinginforecordcategory,
        plant,
        materialplanneddeliverydurn,
        pricevalidityenddate
        -- ingestiontime,
        -- isupsert,
        -- isdelete,
        -- isinsert,
        -- changetype
    FROM silver_mm_ziinforecorgdata
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
        netpriceamount
    FROM silver_mm_zimmpurgdocitem
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
)

SELECT 

    i.purchasinginforecord, -- grain
    i.purchasingorganization, -- grain
    i.purchasinginforecordcategory, -- grain
    i.plant, -- grain
    po.purchasingdocument, -- grain
    po.purchasingdocumentitem, -- grain
    i.materialplanneddeliverydurn,
    i.pricevalidityenddate,
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
    po.taxcode,
    po.safetystockquantity,
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
    current_timestamp() as last_main_silver_modified_dt


    FROM silver_ziinforecorgdata_cte i
    LEFT JOIN silver_zimmpurgdocitem_cte po
    ON i.purchasinginforecord = po.purchasinginforecord
    LEFT JOIN silver_zmmpurchasingdoc_cte h
    ON po.purchasingdocument = h.purchasingdocument