WITH base AS (
    SELECT
        DATE_TRUNC('week', i.order_purchase_timestamp)::DATE AS week_start,
        i.seller_state,
        i.customer_state,
        i.order_id,
        i.item_gmv
    FROM {{ ref('int_order_items_enriched') }} AS i
    WHERE i.order_purchase_timestamp IS NOT NULL
),

experience AS (
    SELECT
        order_id,
        delivery_days,
        late_delivery,
        review_score
    FROM {{ ref('int_orders_experience') }}
),

joined AS (
    SELECT
        b.week_start,
        b.seller_state,
        b.customer_state,
        b.order_id,
        b.item_gmv,
        e.delivery_days,
        e.late_delivery,
        e.review_score
    FROM base AS b
    LEFT JOIN experience AS e
        ON b.order_id = e.order_id
)

SELECT
    week_start,
    seller_state,
    customer_state,

    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(item_gmv), 2) AS gross_revenue,

    ROUND(
        SUM(item_gmv) / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS aov_proxy,

    /* Denominators */
    COUNT(DISTINCT CASE WHEN delivery_days IS NOT NULL THEN order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN review_score  IS NOT NULL THEN order_id END) AS reviewed_orders,

    /* Delivery metrics: computed over delivered_orders */
    ROUND(
        AVG(delivery_days::NUMERIC),
        2
    ) AS avg_delivery_days,

    ROUND(
        SUM(CASE WHEN late_delivery IS NOT NULL AND late_delivery THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(CASE WHEN late_delivery IS NOT NULL THEN 1 END), 0),
        4
    ) AS late_delivery_rate,

    /* Review metrics: computed over reviewed_orders */
    ROUND(
        AVG(review_score::NUMERIC),
        2
    ) AS avg_review_score

FROM joined
GROUP BY
    week_start,
    seller_state,
    customer_state
ORDER BY
    week_start,
    seller_state,
    customer_state
