WITH seq AS (
    SELECT
        customer_unique_id,
        order_id,
        order_purchase_timestamp,
        order_num,
        LEAD(order_purchase_timestamp) OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp, order_id
        ) AS next_order_timestamp
    FROM {{ ref('int_customer_order_sequence') }}
),

first_delivered AS (
    SELECT
        s.customer_unique_id,
        s.order_id AS first_order_id,
        s.order_purchase_timestamp AS first_order_timestamp,
        s.next_order_timestamp
    FROM seq AS s
    INNER JOIN {{ ref('int_orders_experience') }} AS e
        ON s.order_id = e.order_id
    WHERE s.order_num = 1
      AND e.order_delivered_customer_date IS NOT NULL
),

features AS (
    SELECT
        f.customer_unique_id,
        e.late_delivery,
        e.review_score,
        CASE
            WHEN f.next_order_timestamp IS NOT NULL
                 AND f.next_order_timestamp <= f.first_order_timestamp + INTERVAL '90 days'
                THEN 1
            ELSE 0
        END AS repeat_within_90d
    FROM first_delivered AS f
    INNER JOIN {{ ref('int_orders_experience') }} AS e
        ON f.first_order_id = e.order_id
),

bucketed AS (
    SELECT
	late_delivery,
        review_score,
        CASE
	    WHEN review_score IS NULL THEN 'no_review'
            WHEN review_score IN (1,2) THEN 'low_1_2'
            WHEN review_score = 3 THEN 'mid_3'
            ELSE 'high_4_5'
        END AS review_bucket,
        repeat_within_90d
    FROM features
)

SELECT
    late_delivery,
    review_bucket,
    COUNT(*) AS customers,
    ROUND(AVG(repeat_within_90d::NUMERIC), 4) AS repeat_rate_90d
FROM bucketed
GROUP BY
    late_delivery,
    review_bucket
ORDER BY
    late_delivery,
    CASE review_bucket
	WHEN 'no_review' THEN 0
	WHEN 'low_1_2' THEN 1
	WHEN 'mid_3' THEN 2
	ELSE 3
    END