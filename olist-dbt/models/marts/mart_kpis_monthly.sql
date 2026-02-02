WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_purchase_timestamp)::DATE AS month_start,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(gross_order_value), 2) AS gross_revenue,
        ROUND(
            SUM(gross_order_value) / NULLIF(COUNT(DISTINCT order_id), 0),
            2
        ) AS aov
    FROM {{ ref('fct_order_financials') }}
    WHERE order_purchase_timestamp IS NOT NULL
    GROUP BY
        1
)

SELECT
    month_start,
    orders,
    gross_revenue,
    aov,

    ROUND(
        (aov - LAG(aov) OVER (ORDER BY month_start))
        / NULLIF(LAG(aov) OVER (ORDER BY month_start), 0),
        4
    ) AS aov_mom_rate
FROM monthly
ORDER BY
    month_start