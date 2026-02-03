# Shankar’s Analytics Portfolio

This repository showcases end-to-end **data analytics projects** focused on transforming raw, messy data into **decision-ready insights** using industry-standard tools and workflows.

Across projects, I emphasize:
- Clear **business problem framing**
- Thoughtful **data modeling and metric definition**
- Reproducible **SQL and analytics pipelines**
- Stakeholder-ready **dashboards, visuals, and recommendations**

The projects below are designed to mirror how analytics teams operate in real production environments, from ingestion and modeling to insight delivery.

---

## 🛒 [Olist Marketing Analytics](/olist-marketing-analytics)
**Tools:** SQL, dbt, Excel  
**Focus:** Marketing analytics, cohorts & LTV, analytics engineering

An end-to-end marketing analytics project analyzing **98,962 e-commerce orders** from Olist (Brazilian marketplace) between **Jan 2017–Aug 2018** to understand revenue drivers, customer retention, and operational bottlenecks.

**Business problem:**  
What drives revenue today, why does customer lifetime value collapse after the first purchase, and where can growth and operations teams intervene?

**What I did:**
- Preserved the **normalized source schema** for traceability
- Built layered **dbt models** (staging → intermediate → marts)
- Defined reusable **semantic metrics** for revenue, retention, and delivery experience
- Conducted **cohort and LTV analysis** to quantify repeat behavior
- Delivered Excel dashboards for stakeholder reporting

**Key insights:**
- Only **2.1%** of customers repeat within 90 days; LTV is almost entirely first-purchase revenue
- Top 10 categories drive **62.4%** of revenue, signaling high portfolio concentration
- Cross-state deliveries are **6+ days slower** and materially more likely to be late

---

## 🏠 [Boston & NYC Rental Market Analytics](/bos-nyc-rental-market-analytics)
**Tools:** Python, SQL, PostGIS, Power BI  
**Focus:** BI, market analysis, stakeholder decision support

A renter-focused market intelligence project that consolidates **active rental listings** from Realtor and Redfin into a cloud-hosted warehouse and powers an interactive **Power BI Rental Finder** for neighborhood-level affordability and supply analysis across **Boston** and **New York City**.

**Business problem:**  
Rental data is fragmented across platforms, making it difficult for renters to compare affordability, unit mix, and inventory concentration at the neighborhood level under realistic budget constraints.

**What I did:**
- Built a **Python ETL pipeline** to ingest and deduplicate cross-platform rental listings
- Designed a **star-schema analytics model** optimized for BI consumption
- Defined affordability and supply metrics at the listing grain
- Delivered a stakeholder-ready Power BI dashboard and insight deck

**Key insights:**
- NYC median rent is ~**34% higher** than Boston, with 1–3BR units **40–60% more expensive**
- At a **$1.5K budget**, Boston offers **~3× more affordable listings**
- Inventory in both cities is highly concentrated, shaping renter choice more than total supply

---

## 📫 Contact
Email: [shankar.veludandi.02@gmail.com](mailto::shankar.veludandi.02@gmail.com)

LinkedIn: [/shankar-veludandi](https://www.linkedin.com/in/shankar-veludandi-7a5b461b3/)
