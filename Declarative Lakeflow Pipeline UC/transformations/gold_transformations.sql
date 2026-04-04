-- Create a category summary
CREATE OR REFRESH MATERIALIZED VIEW lakeflow_dlt_uc.gold.category_sales_summary AS
SELECT
    product_category,
    YEAR(order_date) AS years,
    SUM(revenue) as total_revenue
FROM lakeflow_dlt_uc.silver.cleaned_sales_data
GROUP BY product_category, YEAR(order_date)
ORDER BY product_category, years;

CREATE OR REFRESH MATERIALIZED VIEW lakeflow_dlt_uc.gold.revenue_by_customers_in_each_region AS
SELECT
    *
FROM (
    SELECT 
        customer_id,
        first_name,
        last_name,
        region_name,
        SUM(revenue) as total_revenue,
        RANK() OVER (PARTITION BY region_name ORDER BY SUM(revenue) DESC) AS revenue_rank
    FROM lakeflow_dlt_uc.silver.cleaned_sales_data 
    GROUP BY customer_id, first_name, last_name, region_name
) ranked_customers
WHERE revenue_rank <= 5
ORDER BY region_name, revenue_rank;

CREATE OR REFRESH MATERIALIZED VIEW lakeflow_dlt_uc.gold.customer_lifetime_Value_Estimation AS
SELECT  
    customer_id,
    first_name,
    last_name, 
    SUM(revenue) as lifetime_revenue,
    COUNT(DISTINCT order_date) as purchase_days,
    AVG(revenue) as avg_revenue_per_purchase
FROM lakeflow_dlt_uc.silver.cleaned_sales_data 
GROUP BY customer_id, first_name, last_name
ORDER BY lifetime_revenue DESC;

-- CREATE OR REFRESH MATERIALIZED VIEW lakeflow_dlt_uc.gold.best_selling_products AS
-- SELECT