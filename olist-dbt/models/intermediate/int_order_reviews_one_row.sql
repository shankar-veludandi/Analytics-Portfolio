WITH ranked_reviews AS (
    SELECT
        order_id,
        review_id,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        CASE
            WHEN NULLIF(TRIM(COALESCE(review_comment_title, '')), '') IS NOT NULL
              OR NULLIF(TRIM(COALESCE(review_comment_message, '')), '') IS NOT NULL
            THEN 1
            ELSE 0
        END AS has_comment_flag,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                review_answer_timestamp DESC NULLS LAST,
                review_creation_date DESC NULLS LAST,
                review_id DESC
        ) AS row_num
    FROM {{ ref('stg_order_reviews') }}
)

SELECT
    order_id,
    review_id,
    review_score,
    review_creation_date,
    review_answer_timestamp,
    has_comment_flag
FROM ranked_reviews
WHERE row_num = 1
