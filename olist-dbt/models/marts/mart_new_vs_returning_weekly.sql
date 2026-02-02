WITH base AS (
    SELECT
        DATE_TRUNC('week', order_purchase_timestamp)::DATE AS week_start,
        order_id,
        customer_unique_id,
        first_order_timestamp
    FROM {{ ref('int_customer_order_sequence') }}
),

rev AS (
    SELECT
        order_id,
        gross_order_value
    FROM {{ ref('fct_order_financials') }}
),

customer_week AS (
    SELECT
        b.week_start,
        b.customer_unique_id,
    
        CASE WHEN DATE_TRUNC('week', MIN(b.first_order_timestamp))::DATE = b.week_start 
             THEN 1 
             ELSE 0 
        END AS is_new_customer_week,

        SUM(COALESCE(r.gross_order_value, 0)) AS customer_week_revenue
    FROM base AS b
    INNER JOIN rev AS r USING(order_id)
    GROUP BY
        b.week_start, b.customer_unique_id
    ORDER BY
        b.week_start
)

SELECT 
    week_start,
    COUNT(*) FILTER (WHERE is_new_customer_week = 1) AS new_customers,
    COUNT(*) FILTER (WHERE is_new_customer_week = 0) AS returning_customers,
    ROUND(COUNT(*) FILTER (WHERE is_new_customer_week = 1)::NUMERIC / NULLIF(COUNT(*), 0), 4) AS new_customer_share,
    SUM(customer_week_revenue) FILTER (WHERE is_new_customer_week = 1) AS new_customer_revenue,
    SUM(customer_week_revenue) FILTER (WHERE is_new_customer_week = 0) AS returning_customer_revenue
FROM customer_week
GROUP BY week_start