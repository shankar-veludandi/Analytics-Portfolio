SELECT
    i.order_id,
    i.order_item_id,
    o.customer_id,
    c.customer_state,
    i.seller_id,
    s.seller_state,
    i.product_id,
    p.product_category_name,
    t.product_category_name_english,
    i.price,
    i.freight_value,
    ROUND(
        COALESCE(i.price, 0) + COALESCE(i.freight_value, 0),
        2
    ) AS item_gmv,
    o.order_purchase_timestamp,
    o.order_status
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('stg_orders') }} o
    ON i.order_id = o.order_id
JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
JOIN {{ ref('stg_sellers') }} s
    ON i.seller_id = s.seller_id
JOIN {{ ref('stg_products') }} p
    ON i.product_id = p.product_id
LEFT JOIN {{ ref('stg_category_translation') }} t
    ON p.product_category_name = t.product_category_name
