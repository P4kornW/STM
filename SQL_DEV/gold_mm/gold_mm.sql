WITH
    silver_zimmpurdocitem_cte AS (
        SELECT DISTINCT
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
        orderquantity,
        taxcode,
    FROM silver_mm_zimmpurdocitem
),

    silver_zmmpurchasingdoc_cte AS (
        SELECT DISTINCT
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

    silver_zipoapprov_cte AS (
        SELECT DISTINCT
        purchasingdocument,
        approve_dt,
        isapprove
    FROM silver_mm_zipoapprov
),

    silver_zipricingelement_cte AS (
        SELECT DISTINCT
        pricingdocument,
        pricingdocumentitem,
        pricingprocedurestep,
        pricingprocedurecounter,
        conditiontype,
        condition_description,
        pricingdatetime,
        conditioncalculationtype,
        conditionamount,
        transactioncurrency,
        conditioninactivereason
    FROM silver_mm_zipricingelement
),

    silver_zipritem_cte AS (
        SELECT DISTINCT
        purchaserequisition,
        purchaserequisitionitem,
        purchasingdocument,
        purchasingdocumentitem
    FROM silver_mm_zipritem

),
    silver_zimmprgdocsl_cte AS (
        SELECT DISTINCT
        purchasingdocument,
        purchasingdocumentitem,
        scheduleline,
        schedulelinedeliverydate
    FROM silver_mm_zimmprgdocsl
),
    silver_zigoodsmvmtdoc_cte AS (
        SELECT DISTINCT
        materialdocumentkey1,
        materialdocumentkey2,
        materialdocumentkey3,
        materialdocumentkey4,
        materialdocumentkey5,
        materialdocumentkey6,
        purchaseorder,
        purchaseorderitem
    FROM silver_mm_zigoodsmvmtdoc
),

    silver_zimmpurdochist_cte AS (
        SELECT DISTINCT
        purchasingdocument,
        purchasingdocumentitem,
        accountassignmentnumber,
        purchasinghistorydocumenttype,
        purchasinghistorydocumentyear,
        purchasinghistorydocument,
        purchasinghistorydocumentitem,
        goodsmovementtype,
        postingdate,
        quantity,
        purordamountincompanycodecrcy,
        purchaseorderamount
    FROM silver_mm_zimmpurdochist

),
    silver_ziinforecorgdata_cte AS (
        SELECT DISTINCT
        purchasinginforecord,
        purchasingorganization,
        purchasinginforecordcategory,
        plant,
        materialplanneddeliverydurn,
        pricevalidityenddate,
    FROM silver_mm_ziinforecorgdata
),