WITH base_po_price AS (
    SELECT
        po.material,
        po.purchasingdocument,
        po.purchasingdocumentitem,
        po.ingestiontime,

        CAST(
            CASE
                WHEN po.netpricequantity IS NULL OR po.netpricequantity = 0
                THEN NULL
                ELSE po.netpriceamount / po.netpricequantity
            END AS DECIMAL(18,3)
        ) AS price_per_unit

    FROM silver_mm_zimmpurgdocitem po
    WHERE po.isdelete = false
      AND po.purchasingdocumentdeletioncode IS NULL
      and po.material IS NOT NULL
),

ranked_latest_price AS (
    SELECT
        b.material,
        b.purchasingdocument,
        b.purchasingdocumentitem,
        d.purchasingdocumentorderdate,
        b.price_per_unit,

        ROW_NUMBER() OVER (
            PARTITION BY b.material
            ORDER BY
                d.purchasingdocumentorderdate DESC,
                b.ingestiontime DESC
        ) AS rn

    FROM base_po_price b
    JOIN silver_mm_zmmpurchasingdoc d
        ON b.purchasingdocument = d.purchasingdocument
    WHERE b.price_per_unit IS NOT NULL
)

SELECT
    material,                                   -- 1. key
    purchasingdocument      AS latest_price_po_document, -- 2
    purchasingdocumentitem  AS latest_price_po_item,     -- 3
    purchasingdocumentorderdate AS latest_price_po_date, -- 4
    price_per_unit          AS latest_material_price_per_unit, -- 5
    current_timestamp()     AS last_modified_dt

FROM ranked_latest_price r
WHERE rn = 1
