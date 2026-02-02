WITH base AS (
    SELECT
        DATE_TRUNC('week', o.order_purchase_timestamp)::DATE AS week_start,
        p.product_category_name,
        t.product_category_name_english,

        COUNT(DISTINCT o.order_id) AS orders,
        COUNT(*) AS items,

        ROUND(SUM(COALESCE(i.price, 0)), 2) AS item_revenue,
        ROUND(SUM(COALESCE(i.freight_value, 0)), 2) AS freight_revenue
    FROM {{ ref('stg_orders') }} AS o
    INNER JOIN {{ ref('stg_order_items') }} AS i
        ON o.order_id = i.order_id
    INNER JOIN {{ ref('stg_products') }} AS p
        ON i.product_id = p.product_id
    LEFT JOIN {{ ref('stg_category_translation') }} AS t
        ON p.product_category_name = t.product_category_name
    WHERE o.order_purchase_timestamp IS NOT NULL
      AND o.order_status = 'delivered'
    GROUP BY
        1, 2, 3
)

SELECT
    week_start,
    COALESCE(product_category_name, 'unknown') AS product_category_name,
    COALESCE(product_category_name_english, 'unknown') AS product_category_name_english,
    orders,
    items,
    ROUND(item_revenue + freight_revenue, 2) AS gross_revenue,
    item_revenue,
    freight_revenue
FROM base
ORDER BY
    week_start,
    gross_revenue DESC