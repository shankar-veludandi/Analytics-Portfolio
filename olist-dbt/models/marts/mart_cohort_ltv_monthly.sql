WITH base_delivered_orders AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp)::DATE AS order_month
    FROM {{ ref('stg_orders') }} AS o
    INNER JOIN {{ ref('stg_customers') }} AS c
        ON o.customer_id = c.customer_id
    WHERE o.order_purchase_timestamp IS NOT NULL
      AND c.customer_unique_id IS NOT NULL
      AND o.order_status = 'delivered'
),

firsts AS (
    SELECT
        customer_unique_id,
        MIN(order_month) AS cohort_month
    FROM base_delivered_orders
    GROUP BY 1
),

joined AS (
    SELECT
        b.order_id,
        b.customer_unique_id,
        f.cohort_month,
        b.order_month,
        (
            (DATE_PART('year', b.order_month) - DATE_PART('year', f.cohort_month)) * 12
          + (DATE_PART('month', b.order_month) - DATE_PART('month', f.cohort_month))
        )::INT AS month_index
    FROM base_delivered_orders AS b
    INNER JOIN firsts AS f
        ON b.customer_unique_id = f.customer_unique_id
),

financials AS (
    SELECT
        order_id,
        gross_order_value
    FROM {{ ref('fct_order_financials') }}
),

cohort_monthly AS (
    SELECT
        j.cohort_month,
        j.month_index,

        COUNT(DISTINCT j.customer_unique_id) AS active_customers,
        COUNT(DISTINCT j.order_id) AS orders,
        ROUND(SUM(COALESCE(f.gross_order_value, 0.0)), 2) AS gross_revenue
    FROM joined AS j
    LEFT JOIN financials AS f
        ON j.order_id = f.order_id
    GROUP BY 1, 2
),

cohort_sizes AS (
    SELECT
        cohort_month,
        MAX(CASE WHEN month_index = 0 THEN active_customers END) AS cohort_size
    FROM cohort_monthly
    GROUP BY 1
)

SELECT
    cm.cohort_month,
    cm.month_index,

    cs.cohort_size,
    cm.active_customers,
    cm.orders,
    cm.gross_revenue,

    -- retention
    ROUND(cm.active_customers::NUMERIC / NULLIF(cs.cohort_size, 0), 4) AS retention_rate,

    -- LTV building blocks (per acquired customer in cohort)
    ROUND(cm.gross_revenue::NUMERIC / NULLIF(cs.cohort_size, 0), 2) AS revenue_per_cohort_customer,

    ROUND(
        SUM(cm.gross_revenue) OVER (
            PARTITION BY cm.cohort_month
            ORDER BY cm.month_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::NUMERIC / NULLIF(cs.cohort_size, 0),
        2
    ) AS cumulative_revenue_per_cohort_customer

FROM cohort_monthly AS cm
INNER JOIN cohort_sizes AS cs
    ON cm.cohort_month = cs.cohort_month
ORDER BY
    cm.cohort_month,
    cm.month_index
