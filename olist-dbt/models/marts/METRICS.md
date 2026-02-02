```markdown

\# Metrics



Conventions:

\- \*\*Rates\*\* are expressed as fractions (0–1) unless explicitly stated otherwise.

\- \*\*Time grain\*\* refers to the aggregation window for the metric (weekly, monthly, segment-level, etc.).

\- If a metric is defined in a downstream mart but ultimately originates from `fct\_order\_financials`, that lineage is noted.



---



\## orders



\- Definition: count of distinct orders in the time period.

\- Source model: `mart\_kpis\_weekly`, `mart\_kpis\_monthly`

\- Time grain: weekly (`week\_start`) or monthly (`month\_start`)

\- Notes: typically counted as `COUNT(DISTINCT order\_id)`.



---



\## gross\_order\_value



\- Definition: GMV-style order value, defined as item revenue plus freight.

\- Source model: `fct\_order\_financials`

\- Time grain: order-level (no time aggregation)

\- Notes: computed as `SUM(price) + SUM(freight\_value)` over order items for a given `order\_id`. Use this as the canonical “order economics” numerator for revenue-style KPIs.



---



\## aov



\- Definition: average gross order value per order in the period.

\- Source model: `mart\_kpis\_weekly`, `mart\_kpis\_monthly` (derived from `fct\_order\_financials`)

\- Time grain: weekly (`week\_start`) or monthly (`month\_start`)

\- Notes: computed as `SUM(gross\_order\_value) / COUNT(DISTINCT order\_id)`.



---



\## delivered\_rate



\- Definition: fraction of orders with `order\_status = 'delivered'` in the period.

\- Source model: `mart\_kpis\_weekly`, `mart\_kpis\_monthly`

\- Time grain: weekly or monthly

\- Notes: value range 0–1. Denominator is total orders in the period.



---



\## canceled\_rate



\- Definition: fraction of orders with `order\_status = 'canceled'` in the period.

\- Source model: `mart\_kpis\_weekly`, `mart\_kpis\_monthly`

\- Time grain: weekly or monthly

\- Notes: value range 0–1. Denominator is total orders in the period.



---



\## rolling\_4w\_orders\_avg



\- Definition: trailing 4-week average of weekly orders.

\- Source model: `mart\_kpis\_weekly`

\- Time grain: weekly

\- Notes: intended for smoothing volatility. If you use this in Excel, keep the window definition consistent (current week + previous 3 weeks).



---



\## rolling\_4w\_aov\_avg



\- Definition: trailing 4-week average of weekly AOV.

\- Source model: `mart\_kpis\_weekly`

\- Time grain: weekly

\- Notes: smoothing metric; do not compare directly to monthly AOV without acknowledging the different grain.



---



\## aov\_mom\_change



\- Definition: month-over-month change in AOV (current month AOV minus prior month AOV, or percent change depending on your model implementation).

\- Source model: `mart\_kpis\_monthly`

\- Time grain: monthly

\- Notes: confirm whether your implementation is absolute delta vs percent change; document that explicitly here once finalized.



---



\## active\_customers



\- Definition: count of distinct active customers (`customer\_unique\_id`) in the cohort at the given `month\_index`.

\- Source model: `mart\_cohorts\_monthly`

\- Time grain: cohort-month by month-index

\- Notes: grain is `(cohort\_month, month\_index)`. This is the primary input for cohort retention heatmaps and curves.



---



\## new\_customers



\- Definition: count of distinct customers whose first-ever order occurs in the week.

\- Source model: `mart\_new\_vs\_returning\_weekly` (derived from `int\_customer\_order\_sequence`)

\- Time grain: weekly

\- Notes: “new” is based on `customer\_unique\_id` lifecycle sequencing.



---



\## returning\_customers



\- Definition: count of distinct customers who have ordered before and place an order in the week.

\- Source model: `mart\_new\_vs\_returning\_weekly` (derived from `int\_customer\_order\_sequence`)

\- Time grain: weekly

\- Notes: returning customers exclude first-time customers for that week.



---



\## new\_customer\_share



\- Definition: fraction of weekly active customers who are new.

\- Source model: `mart\_new\_vs\_returning\_weekly`

\- Time grain: weekly

\- Notes: value range 0–1. Computed as `new\_customers / (new\_customers + returning\_customers)` (or equivalent denominator in your model).



---



\## category\_orders



\- Definition: count of distinct orders associated with a given product category in the week.

\- Source model: `mart\_category\_weekly`

\- Time grain: weekly by category

\- Notes: grain is `(week\_start, product\_category\_name)`. If your mart filters to delivered orders, this metric is “delivered category orders” by design.



---



\## category\_revenue



\- Definition: gross order value attributable to a product category in the week.

\- Source model: `mart\_category\_weekly` (derived from items + order economics logic)

\- Time grain: weekly by category

\- Notes: if attribution is based on item-level sums, clarify whether revenue is item-level revenue (plus freight) or item-only. Align with how `gross\_order\_value` is defined.



---



\## flow\_orders



\- Definition: count of distinct orders for a given seller\_state → customer\_state flow in the week.

\- Source model: `mart\_geo\_state\_flows\_weekly`

\- Time grain: weekly by (seller\_state, customer\_state)

\- Notes: grain is `(week\_start, seller\_state, customer\_state)`.



---



\## flow\_revenue



\- Definition: gross order value for a given seller\_state → customer\_state flow in the week.

\- Source model: `mart\_geo\_state\_flows\_weekly`

\- Time grain: weekly by (seller\_state, customer\_state)

\- Notes: ensure this is consistent with `gross\_order\_value` and whether freight is included.



---



\## aov\_proxy



\- Definition: revenue per distinct order for the flow in the week.

\- Source model: `mart\_geo\_state\_flows\_weekly`

\- Time grain: weekly by (seller\_state, customer\_state)

\- Notes: computed as `flow\_revenue / flow\_orders`. Named “proxy” because it is flow-aggregated, not a customer-level AOV.



---



\## late\_delivery



\- Definition: boolean indicator of whether the first delivered order arrived after the estimated delivery date.

\- Source model: `mart\_experience\_retention\_90d`

\- Time grain: segment-level

\- Notes: segment key for experience. Grain is `(late\_delivery, review\_score)`.



---



\## review\_score



\- Definition: review score value used to segment first-order experience.

\- Source model: `mart\_experience\_retention\_90d`

\- Time grain: segment-level

\- Notes: expected to be a small integer scale (commonly 1–5). Grain is `(late\_delivery, review\_score)`.



---



\## customers



\- Definition: count of customers (`customer\_unique\_id`) in the experience segment.

\- Source model: `mart\_experience\_retention\_90d`

\- Time grain: segment-level

\- Notes: denominator for segment-level repeat behavior. Grain is `(late\_delivery, review\_score)`.



---



\## repeat\_rate\_90d



\- Definition: fraction of customers who place another order within 90 days of their first delivered order.

\- Source model: `mart\_experience\_retention\_90d`

\- Time grain: segment-level

\- Notes: value range 0–1. Based on first delivered order only; confirm that “repeat” counts any subsequent order within 90 days of delivery date (not purchase date) to remain consistent.



---



\## Suggested next additions (optional)



If you want this contract to feel “production-ready” without adding the full Semantic Layer:

\- Add an \*\*Owner\*\* field per metric (e.g., “Analytics/BI”).

\- Add a \*\*Certification\*\* status (draft/active/deprecated).

\- Add a short \*\*SQL definition\*\* block for 3–5 highest-impact metrics (AOV, delivered\_rate, repeat\_rate\_90d).

```



