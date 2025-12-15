# Boston & NYC Rental Market Analytics

End-to-end rental market analytics project for **Boston** and **New York City** built to showcase my skills as a **Data Analyst** in data engineering, analysis, and stakeholder reporting. 

The project pulls live listings from Realtor and Redfin APIs into Supabase; cleans, transforms, and imputes the data; and automatically refreshes an interactive Power BI dashboard weekly.

---

## Business Problem

Boston and New York City are two of the most expensive rental markets in the U.S., and renters struggle to answer basic questions like where they can afford to live and how many viable options they actually have.

This project asks:

- How do rents compare between Boston and NYC across neighborhoods and bedroom sizes?
- Given a renter’s **budget, bedroom, square footage, and pet-policy requirements**, which neighborhoods are feasible?
- How many active listings meet those criteria today, and how are they distributed across each city?
- Where do “affordable” versus “premium” pockets of each market emerge?

---

## Key Outcomes

- **Combined 20K+ active rental listings** across Boston and NYC stored in a cloud-hosted PostgreSQL data warehouse.
- **Showcased key metrics and visuals** summarizing each rental market across city and neighborhood levels on affordability and availability.
- **Delivered actionable insights** clearly and effectively through consulting-style data storytelling.

---

## Skills Demonstrated

- **Python for ETL** – API ingestion, error handling, logging, and orchestration of the full pipeline.
- **SQL for Data Cleaning** – aggregations, multi-step transformations, imputation, and outlier handling.
- **Power BI for Data Visualization** – DAX, Power Query, and dashboard UI/UX.
- **Cloud Data Warehousing** – multi-layer schemas (`raw → staging → cleaned → analytics`), environment-based credential management, and efficient refresh patterns in a centralized cloud database.
- **Automation** – single entry-point pipeline plus a scheduled batch job for weekly refreshes.

## For Recruiters & Hiring Managers

If you only have a few minutes, I recommend:

1. Download and open the [Power BI report](./powerbi/NYC& BOS Rental Market Analysis.pbix) to check out the rental market overview and rental finder dashboards.  
2. Download and skim the [PowerPoint slide deck](./docs/Boston & NYC Rental Market Insights.pptx) to view the business story and insights.  
3. Glance at the Python and SQL scripts to see how data is fetched from APIs and cleaned and transformed into tables ready for data analysis.

This project proves I can turn messy real-world data into insights and narratives that help renters based in Boston and New York City make better decisions about where to live.
