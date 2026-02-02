# Metrics

Conventions:
- **Rates** are expressed as fractions (0–1) unless explicitly stated otherwise.
- **Time grain** refers to the aggregation window for the metric (weekly, cohort-month by index, segment-level, etc.).
- If a metric is defined in a downstream mart but ultimately originates from `fct_order_financials`, that lineage is noted.

---

## orders
- Definition: count of distinct orders in the time period.
- Source model: `mart_kpis_weekly`
- Time grain: weekly (`week_start`)
- Notes: typically counted as `COUNT(DISTINCT order_id)`.

---

## gross_order_value
- Definition: GMV-style order value, defined as item revenue plus freight.
- Source model: `fct_order_financials`
- Time grain: order-level (no time aggregation)
- Notes: computed as `SUM(price) + SUM(freight_value)` over order items for a given `order_id`. Use this as the canonical “order economics” numerator for revenue-style KPIs.

---

## aov
- Definition: average gross order value per order in the period.
- Source model: `mart_kpis_weekly` (derived from `fct_order_financials`)
- Time grain: weekly (`week_start`)
- Notes: computed as `SUM(gross_order_value) / COUNT(DISTINCT order_id)`.

---

## delivered_rate
- Definition: fraction of orders with `order_status = 'delivered'` in the period.
- Source model: `mart_kpis_weekly`
- Time grain: weekly
- Notes: value range 0–1. Denominator is total orders in the period.

---

## canceled_rate
- Definition: fraction of orders with `order_status = 'canceled'` in the period.
- Source model: `mart_kpis_weekly`
- Time grain: weekly
- Notes: value range 0–1. Denominator is total orders in the period.

---

## rolling_4w_orders_avg
- Definition: trailing 4-week average of weekly orders.
- Source model: `mart_kpis_weekly`
- Time grain: weekly
- Notes: intended for smoothing volatility. Keep the window definition consistent (current week + previous 3 weeks).

---

## rolling_4w_aov_avg
- Definition: trailing 4-week average of weekly AOV.
- Source model: `mart_kpis_weekly`
- Time grain: weekly
- Notes: smoothing metric; do not compare directly to monthly AOV without acknowledging the different grain.

---

## cohort_month
- Definition: the acquisition cohort month for a customer, defined as the month of the customer's first delivered order.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: cohort grouping key for retention and LTV analysis.

---

## month_index
- Definition: number of months since `cohort_month` (0 = acquisition month).
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: the column index for cohort heatmaps and retention/LTV curves.

---

## cohort_size
- Definition: number of acquired customers in the cohort (active_customers at `month_index = 0`).
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month
- Notes: denominator for retention and LTV-per-acquired-customer calculations.

---

## active_customers
- Definition: count of distinct active customers (`customer_unique_id`) in the cohort at the given `month_index`.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: primary input for cohort retention heatmaps and curves. Grain is `(cohort_month, month_index)`.

---

## cohort_orders
- Definition: count of delivered orders placed by cohort customers at the given `month_index`.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: useful for purchase-frequency interpretation alongside revenue.

---

## cohort_gross_revenue
- Definition: sum of `gross_order_value` for delivered orders placed by cohort customers at the given `month_index`.
- Source model: `mart_cohort_ltv_monthly` (derived from `fct_order_financials`)
- Time grain: cohort-month by month-index
- Notes: revenue proxy consistent with exec dashboard revenue definition.

---

## retention_rate
- Definition: fraction of the cohort active at the given month_index.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: computed as `active_customers / cohort_size`. Value range 0–1.

---

## revenue_per_cohort_customer
- Definition: gross revenue per acquired customer for the given month_index.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: computed as `cohort_gross_revenue / cohort_size`. This is the month-by-month LTV contribution per acquired customer.

---

## cumulative_revenue_per_cohort_customer
- Definition: cumulative gross revenue per acquired customer up to the given month_index.
- Source model: `mart_cohort_ltv_monthly`
- Time grain: cohort-month by month-index
- Notes: computed as cumulative SUM of `cohort_gross_revenue` over month_index divided by cohort_size. This is the LTV curve.

---

## new_customers
- Definition: count of distinct customers whose first-ever order occurs in the week.
- Source model: `mart_new_vs_returning_weekly` (derived from `int_customer_order_sequence`)
- Time grain: weekly
- Notes: “new” is based on `customer_unique_id` lifecycle sequencing.

---

## returning_customers
- Definition: count of distinct customers who have ordered before and place an order in the week.
- Source model: `mart_new_vs_returning_weekly` (derived from `int_customer_order_sequence`)
- Time grain: weekly
- Notes: returning customers exclude first-time customers for that week.

---

## new_customer_share
- Definition: fraction of weekly active customers who are new.
- Source model: `mart_new_vs_returning_weekly`
- Time grain: weekly
- Notes: value range 0–1. Computed as `new_customers / (new_customers + returning_customers)` (or equivalent denominator in your model).

---

## category_orders
- Definition: count of distinct orders associated with a given product category in the week.
- Source model: `mart_category_weekly`
- Time grain: weekly by category
- Notes: grain is `(week_start, product_category_name)`. If your mart filters to delivered orders, this metric is “delivered category orders” by design.

---

## category_revenue
- Definition: gross order value attributable to a product category in the week.
- Source model: `mart_category_weekly` (derived from items + order economics logic)
- Time grain: weekly by category
- Notes: align with how `gross_order_value` is defined and whether freight is included.

---

## flow_orders
- Definition: count of distinct orders for a given seller_state → customer_state flow in the week.
- Source model: `mart_geo_state_flows_weekly`
- Time grain: weekly by (seller_state, customer_state)
- Notes: grain is `(week_start, seller_state, customer_state)`.

---

## flow_revenue
- Definition: gross order value for a given seller_state → customer_state flow in the week.
- Source model: `mart_geo_state_flows_weekly`
- Time grain: weekly by (seller_state, customer_state)
- Notes: ensure this is consistent with `gross_order_value` and whether freight is included.

---

## aov_proxy
- Definition: revenue per distinct order for the flow in the week.
- Source model: `mart_geo_state_flows_weekly`
- Time grain: weekly by (seller_state, customer_state)
- Notes: computed as `flow_revenue / flow_orders`. Named “proxy” because it is flow-aggregated, not a customer-level AOV.

---

## late_delivery
- Definition: boolean indicator of whether the first delivered order arrived after the estimated delivery date.
- Source model: `mart_experience_retention_90d`
- Time grain: segment-level
- Notes: segment key for experience. Grain is `(late_delivery, review_score)`.

---

## review_score
- Definition: review score value used to segment first-order experience.
- Source model: `mart_experience_retention_90d`
- Time grain: segment-level
- Notes: expected to be a small integer scale (commonly 1–5). Grain is `(late_delivery, review_score)`.

---

## customers
- Definition: count of customers (`customer_unique_id`) in the experience segment.
- Source model: `mart_experience_retention_90d`
- Time grain: segment-level
- Notes: denominator for segment-level repeat behavior. Grain is `(late_delivery, review_score)`.

---

## repeat_rate_90d
- Definition: fraction of customers who place another order within 90 days of their first delivered order.
- Source model: `mart_experience_retention_90d`
- Time grain: segment-level
- Notes: value range 0–1. Based on first delivered order only; confirm whether “repeat” is based on delivery date vs purchase date to remain consistent.
