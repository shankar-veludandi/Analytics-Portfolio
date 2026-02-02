SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_purchase_timestamp IS NOT NULL
        THEN o.order_delivered_customer_date::DATE
           - o.order_purchase_timestamp::DATE
    END AS delivery_days,
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_estimated_delivery_date IS NOT NULL
        THEN o.order_delivered_customer_date::DATE
           > o.order_estimated_delivery_date::DATE
    END AS late_delivery,
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_estimated_delivery_date IS NOT NULL
        THEN o.order_delivered_customer_date::DATE
           - o.order_estimated_delivery_date::DATE
    END AS days_vs_estimated,
    r.review_score,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.has_comment_flag
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('int_order_reviews_one_row') }} r
    ON o.order_id = r.order_id