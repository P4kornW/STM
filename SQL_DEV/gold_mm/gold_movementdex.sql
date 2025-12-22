 WITH silver_zigoodsmvmtdoc_cte AS (
        SELECT
        materialdocumentkey1,
        materialdocumentkey2,
        materialdocumentkey3,
        materialdocumentkey4,
        materialdocumentkey5,
        materialdocumentkey6,
        purchaseorder,
        purchaseorderitem,
        postingdate
    FROM silver_mm_zigoodsmvmtdoc
),

    silver_zimmpurdochist_cte AS (
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
        quantity,
        purordamountincompanycodecrcy,
        purchaseorderamount
    FROM silver_mm_zimmpurdochist
)
SELECT 

    ph.purchasingdocument,
    ph.purchasingdocumentitem,
    ph.goodsmovementtype,
    ph.postingdate,
    ph.quantity,
    ph.purordamountincompanycodecrcy,
    ph.purchaseorderamount,
    md.materialdocumentkey1,
    md.materialdocumentkey2,
    md.materialdocumentkey3,
    md.materialdocumentkey4,
    md.materialdocumentkey5,
    md.materialdocumentkey6

    
    FROM silver_zimmpurdochist_cte ph
    LEFT JOIN silver_zigoodsmvmtdoc_cte md
    ON ph.purchasingdocument = md.purchaseorder
    AND ph.purchasingdocumentitem = md.purchaseorderitem
    AND ph.postingdate = md.postingdate