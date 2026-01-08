 WITH silver_zimmpurdochist_cte AS (
        SELECT
        purchasingdocument,
        purchasingdocumentitem,
        accountassignmentnumber,
        purchasinghistorydocumenttype,
        purchasinghistorydocumentyear,
        purchasinghistorydocument,
        purchasinghistorydocumentitem,
        goodsmovementtype,
        postingdate,
        debitcreditcode,
        quantity,
        purordamountincompanycodecrcy,
        purchaseorderamount,
        ingestiontime,
        isinsert,
        isupsert,
        isdelete,
        changetype
    FROM silver_mm_zimmpurdochist WHERE isdelete = false

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
        netpriceamount,
        netamount,
        netpricequantity
    FROM silver_mm_zimmpurgdocitem WHERE isdelete = false AND purchasingdocumentdeletioncode IS NULL
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
    FROM silver_mm_zmmpurchasingdoc WHERE isdelete = false
)

SELECT 

    h.purchasingdocument, -- grain
    h.purchasingdocumentitem, -- grain
    h.accountassignmentnumber, -- grain
    h.purchasinghistorydocumenttype, -- grain
    h.purchasinghistorydocumentyear, -- grain
    h.purchasinghistorydocument, -- grain
    h.purchasinghistorydocumentitem, -- grain
    h.goodsmovementtype,
    h.postingdate,
    h.debitcreditcode,
    h.quantity,
    h.purordamountincompanycodecrcy,
    h.purchaseorderamount,
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
    hd.supplier,
    hd.suppliername,
    hd.releasecode,
    hd.purchasinggroup,
    hd.documentcurrency,
    hd.exchangerate,
    hd.purchasingdocumentorderdate,
    hd.paymentterms,
    hd.incotermsclassification,
    hd.purchasingdocumentcondition,
    hd.purgreleasetimetotalamount,
    h.ingestiontime,
    h.isinsert,
    h.isupsert,
    h.isdelete,
    h.changetype,
    current_timestamp() as last_main_silver_modified_dt

    FROM silver_zimmpurdochist_cte h
    LEFT JOIN silver_zimmpurgdocitem_cte po
    ON h.purchasingdocument = po.purchasingdocument
    AND h.purchasingdocumentitem = po.purchasingdocumentitem
    LEFT JOIN silver_zmmpurchasingdoc_cte hd
    ON po.purchasingdocument = hd.purchasingdocument