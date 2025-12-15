# Boston & NYC Rental Market Analytics

End-to-end rental market analytics project for **Boston** and **New York City** built to showcase my skills as a **Data Analyst** in data engineering, analysis, and stakeholder reporting.

**Type:** Market Analytics · Geospatial Analytics · Dashboarding  
**Tech:** Python, SQL (PostgreSQL/PostGIS), Supabase, Power BI

---

## Business Problem

Boston and New York City are two of the most expensive rental markets in the U.S., and renters struggle to answer basic questions like where they can afford to live and how many viable options they actually have.

This project asks:

- How do rents compare between Boston and NYC across neighborhoods and bedroom sizes?
- Given a renter’s **budget, bedroom, square footage, and pet-policy requirements**, which neighborhoods are feasible?
- How many active listings meet those criteria today, and how are they distributed across each city?
- Where do “affordable” versus “premium” pockets of each market emerge?

---

## Data & Scope

- **Sources:** Realtor and Redfin rental listing APIs  
- **Warehouse:** Supabase-hosted PostgreSQL with PostGIS extensions  
- **Coverage:** Active listings for Boston and NYC, refreshed weekly  
- **Granularity:** Listing-level data (one row per active unit)  
- **Key fields:** Rent, beds, baths, sqft, pets allowed, neighborhood / NTA, latitude/longitude, URLs

---

## Approach

- **Data Engineering & Cleaning**
  - Built Python ETL scripts to pull listings from Realtor and Redfin, with basic retry logic, logging, and error handling.
  - Modeled the warehouse using multi-layer schemas: `raw → staging → cleaned → analytics`.
  - Deduplicated across sources using address, geolocation, and price bands; imputed missing square footage using median sqft by ZIP + beds + baths.
  - Applied outlier rules (percentile-based fences) to remove extreme rents, sqft, and bedroom/bathroom values.

- **Analysis & Modeling**
  - Joined NYC listings to official NTA boundary polygons using PostGIS to enable neighborhood-level mapping.
  - Engineered metrics such as **rent per bedroom**, **affordability tiers**, and **pet-friendly premiums**.
  - Designed **Power BI dashboards**:
    - City-level overviews (KPIs and neighborhood summary tables).
    - Rental Finder pages where users filter by budget, beds/baths, sqft, neighborhood, and pet policy.
  - Automated weekly refresh via a single Python entry-point plus a scheduled batch job.

---

## Key Findings

- NYC rents are **materially higher** than Boston overall and on a per-bedroom basis, with particularly large gaps for smaller units (studios and 1BRs).
- Boston’s supply includes more 3–5 bedroom units, while NYC’s inventory is more heavily concentrated in studios and 1BRs.
- Pet-friendly units command a measurable rent premium in both cities, especially in high-demand neighborhoods.
- The dashboards make it easy for a renter to translate vague questions (“Can I afford Brooklyn with a dog?”) into concrete options and trade-offs.

---

## Skills Demonstrated

- **Python for ETL** – API ingestion, error handling, logging, and orchestration of the full pipeline via a single driver script.
- **SQL for Data Cleaning** – multi-step transformations, aggregations, median-based imputation, and percentile-based outlier handling.
- **Power BI for Data Visualization** – DAX measures, Power Query transformations, and interactive dashboard design (filters, drill-downs, map visuals).
- **Cloud Data Warehousing** – multi-layer schemas (`raw → staging → cleaned → analytics`), environment-based credential management, and efficient refresh patterns in a centralized Supabase/PostgreSQL database.
- **Automation** – weekly, unattended refresh using a scheduled batch job that runs the pipeline end-to-end.
- **Geospatial Analytics** – use of PostGIS and NTA boundary data to attach neighborhood context and support map-based decision making.

---

## For Recruiters & Hiring Managers

If you only have a few minutes, I recommend:

1. Download and open the [Power BI dashboard](./powerbi/NYC%20&%20BOS%20Rental%20Market%20Analysis.pbix) to check out the rental market overview and rental finder dashboards.  
2. Download and skim the [PowerPoint presentation](./docs/Boston%20&%20NYC%20Rental%20Market%20Insights.pptx) to view the business story and insights.  
3. Glance at the Python and SQL scripts to see how data is fetched from APIs and cleaned and transformed into tables ready for data analysis.
