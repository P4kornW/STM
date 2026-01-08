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
    FROM silver_mm_ziinforecorgdata where isdelete = false
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
    FROM silver_mm_zimmpurgdocitem where isdelete = false AND purchasingdocumentdeletioncode IS NULL
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
    FROM silver_mm_zmmpurchasingdoc where isdelete = false
)

SELECT 

    i.purchasinginforecord, -- grain
    i.purchasingorganization, -- grain
    i.purchasinginforecordcategory,  -- grain
    i.plant,  -- grain
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
    po.netpricequantity,
    po.netamount,
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
    h.purgreleasetimetotalamount,
    i.ingestiontime,
    i.isinsert,
    i.isupsert,
    i.isdelete,
    i.changetype,
    current_timestamp() as last_main_silver_modified_dt


    FROM silver_ziinforecorgdata_cte i
    JOIN silver_zimmpurgdocitem_cte po
    ON i.purchasinginforecord = po.purchasinginforecord
    JOIN silver_zmmpurchasingdoc_cte h
    ON po.purchasingdocument = h.purchasingdocument