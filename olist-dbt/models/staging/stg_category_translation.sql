SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('olist_raw', 'raw_product_category_name_translation') }}
