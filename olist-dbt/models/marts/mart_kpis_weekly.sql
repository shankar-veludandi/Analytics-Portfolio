WITH base AS (
    SELECT
        DATE_TRUNC('week', order_purchase_timestamp)::DATE AS week_start,
        order_status,
        gross_order_value,
        item_revenue,
        freight_revenue,
        item_rows
    FROM {{ ref('fct_order_financials') }}
    WHERE order_purchase_timestamp IS NOT NULL
),

agg AS (
    SELECT
        week_start,
        COUNT(*) AS orders,
        ROUND(SUM(gross_order_value), 2) AS gross_revenue,
        ROUND(SUM(item_revenue), 2) AS item_revenue,
        ROUND(SUM(freight_revenue), 2) AS freight_revenue,

        ROUND(AVG(item_revenue), 2) AS aov,
	ROUND(AVG(gross_order_value), 2) AS order_value_incl_shipping,
        ROUND(AVG(freight_revenue), 2)   AS shipping_per_order,
        ROUND(SUM(freight_revenue) / NULLIF(SUM(gross_order_value), 0), 4) AS shipping_share,

        ROUND(AVG(item_rows::NUMERIC), 2) AS items_per_order,



        ROUND(
            SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0),
            4
        ) AS delivered_rate,

        ROUND(
            SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0),
            4
        ) AS canceled_rate
    FROM base
    GROUP BY
        week_start
)

SELECT
    week_start,
    orders,
    gross_revenue,
    item_revenue,
    freight_revenue,
    aov,
    order_value_incl_shipping,
    shipping_per_order,
    shipping_share,
    items_per_order,
    delivered_rate,
    canceled_rate,

    ROUND(
        AVG(orders::NUMERIC) OVER (
            ORDER BY week_start
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS orders_4wk_avg,

    ROUND(
        AVG(gross_revenue) OVER (
            ORDER BY week_start
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS revenue_4wk_avg,

    ROUND(
        AVG(aov) OVER (
            ORDER BY week_start
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS aov_4wk_avg
FROM agg
ORDER BY
    week_start
