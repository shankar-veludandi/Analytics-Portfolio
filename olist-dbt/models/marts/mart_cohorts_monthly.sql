WITH orders AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp)::DATE AS order_month
    FROM {{ ref('stg_orders') }} AS o
    INNER JOIN {{ ref('stg_customers') }} AS c
        ON o.customer_id = c.customer_id
    WHERE o.order_purchase_timestamp IS NOT NULL
      AND c.customer_unique_id IS NOT NULL
),

firsts AS (
    SELECT
        customer_unique_id,
        MIN(order_month) AS cohort_month
    FROM orders
    GROUP BY
        customer_unique_id
),

joined AS (
    SELECT
        o.customer_unique_id,
        f.cohort_month,
        o.order_month,
        (
            (DATE_PART('year', o.order_month) - DATE_PART('year', f.cohort_month)) * 12
            + (DATE_PART('month', o.order_month) - DATE_PART('month', f.cohort_month))
        )::INT AS month_index
    FROM orders AS o
    INNER JOIN firsts AS f
        ON o.customer_unique_id = f.customer_unique_id
)

SELECT
    cohort_month,
    month_index,
    COUNT(DISTINCT customer_unique_id) AS active_customers
FROM joined
GROUP BY
    cohort_month,
    month_index
ORDER BY
    cohort_month,
    month_index