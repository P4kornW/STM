WITH Ranked_Raw_Batch AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tabkey
            ORDER BY ingestiontime DESC
        ) AS rn
    FROM ziprapprov
    WHERE ingestiontime >= (select coalesce(max(ingestiontime),'1900-01-01') - INTERVAL 6 HOUR from silver_mm_ziprapprov)
        AND tabkey IS NOT NULL
        -- AND isdelete = false
)

SELECT
    CAST(R.tabkey AS STRING) AS tabkey,
    CAST(R.purchasereqnitemuniqueid AS STRING) AS purchasereqnitemuniqueid,
    to_date(R.updatedate, 'yyyyMMdd') AS updatedate,
    CAST(R.updatetime AS STRING) AS updatetime,
    TO_TIMESTAMP(
        CONCAT(R.updatedate, R.updatetime),
        'yyyyMMddHHmmss'
    ) AS update_dt,
    R.ingestiontime,
    R.isupsert,
    R.isdelete,
    R.isinsert,
    R.changetype,
    current_timestamp() AS last_modified_dt
FROM Ranked_Raw_Batch R
WHERE rn = 1
