WITH items AS (
    SELECT
        order_id,
        COUNT(*) AS item_rows,
        COUNT(DISTINCT product_id) AS distinct_products,
        COUNT(DISTINCT seller_id) AS distinct_sellers,
        SUM(COALESCE(price, 0)) AS item_revenue,
        SUM(COALESCE(freight_value, 0)) AS freight_revenue
    FROM {{ ref('stg_order_items') }}
    GROUP BY
        order_id
),

payments AS (
    SELECT
        order_id,
        COUNT(*) AS payment_rows,
        SUM(COALESCE(payment_value, 0)) AS payment_total
    FROM {{ ref('stg_order_payments') }}
    GROUP BY
        order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    COALESCE(i.item_rows, 0) AS item_rows,
    i.distinct_products,
    i.distinct_sellers,

    ROUND(COALESCE(i.item_revenue, 0), 2)    AS item_revenue,
    ROUND(COALESCE(i.freight_revenue, 0), 2) AS freight_revenue,

    p.payment_rows,
    ROUND(p.payment_total, 2) AS payment_total,

    ROUND(COALESCE(i.item_revenue, 0) + COALESCE(i.freight_revenue, 0), 2) AS gross_order_value
FROM {{ ref('stg_orders') }} AS o
LEFT JOIN items AS i
    ON o.order_id = i.order_id
LEFT JOIN payments AS p
    ON o.order_id = p.order_id