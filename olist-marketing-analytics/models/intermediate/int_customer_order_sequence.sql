WITH orders_clean AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_purchase_timestamp
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE c.customer_unique_id IS NOT NULL
      AND o.order_purchase_timestamp IS NOT NULL
),
numbered AS (
    SELECT
        customer_id,
        customer_unique_id,
        order_id,
        order_purchase_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp, order_id
        ) AS order_num,
        MIN(order_purchase_timestamp) OVER (
            PARTITION BY customer_unique_id
        ) AS first_order_timestamp,
        LAG(order_purchase_timestamp) OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp, order_id
        ) AS prev_order_timestamp
    FROM orders_clean
)

SELECT
    customer_id,
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    order_num,
    first_order_timestamp,
    prev_order_timestamp,
    order_purchase_timestamp::DATE
        - prev_order_timestamp::DATE AS days_since_previous_order,
    CASE
        WHEN order_num = 1 THEN 1
        ELSE 0
    END AS is_first_order_flag
FROM numbered
