CREATE TABLE public.superstore (
    order_date DATE,
    customer_name VARCHAR(100),
    sales NUMERIC(10,2),
    profit NUMERIC(10,2)
);

-- Total sales per customer
SELECT
    "CUSTOMER_NAME",
    SUM(sales) AS total_sales
FROM public.superstore
GROUP BY "CUSTOMER_NAME"
ORDER BY total_sales DESC;


--  Rank customers by sales per region
SELECT
    "REGION",
    "CUSTOMER_NAME",
    total_sales,
    ROW_NUMBER() OVER (
        PARTITION BY "REGION"
        ORDER BY total_sales DESC
    ) AS row_num
FROM (
    SELECT
        "REGION",
        "CUSTOMER_NAME",
        SUM("sales") AS total_sales
    FROM public.superstore
    GROUP BY "REGION", "CUSTOMER_NAME"
) 


--RANK vs DENSE_RANK
SELECT
    "REGION",
    "CUSTOMER_NAME",
    total_sales,
    RANK() OVER (
        PARTITION BY "REGION"
        ORDER BY total_sales DESC
    ) AS rank_val,
    DENSE_RANK() OVER (
        PARTITION BY "REGION"
        ORDER BY total_sales DESC
    ) AS dense_rank_val
FROM (
    SELECT
        "REGION",
        "CUSTOMER_NAME",
        SUM("sales") AS total_sales
    FROM public.superstore
    GROUP BY "REGION", "CUSTOMER_NAME"
) t
ORDER BY "REGION", rank_val;

--Running total of sales
SELECT
    "ORDER_DATE",
    "sales",
    SUM("sales") OVER (
        ORDER BY "ORDER_DATE"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_sales
FROM public.superstore
ORDER BY "ORDER_DATE";

-- Monthly Sales using mixed date formats
   -- Handles MM/DD/YYYY and YYYY-MM-DD
   -- Converts ORDER_DATE to DATE safely
SELECT
    DATE_TRUNC(
        'month',
        CASE
            WHEN "ORDER_DATE" LIKE '%/%'
                THEN TO_DATE("ORDER_DATE", 'MM/DD/YYYY')
            ELSE
                TO_DATE("ORDER_DATE", 'YYYY-MM-DD')
        END
    ) AS month,
    SUM("sales") AS monthly_sales
FROM public.superstore
GROUP BY month
ORDER BY month;

--Month-over-Month (MoM) Growth using LAG
   -- Calculates previous month sales
   -- Finds growth or decline
SELECT
    month,
    monthly_sales,
    LAG(monthly_sales) OVER (ORDER BY month) AS prev_month_sales,
    monthly_sales - LAG(monthly_sales) OVER (ORDER BY month) AS mom_growth
FROM (
    SELECT
        DATE_TRUNC(
            'month',
            CASE
                WHEN "ORDER_DATE" LIKE '%/%'
                    THEN TO_DATE("ORDER_DATE", 'MM/DD/YYYY')
                ELSE
                    TO_DATE("ORDER_DATE", 'YYYY-MM-DD')
            END
        ) AS month,
        SUM("sales") AS monthly_sales
    FROM public.superstore
    GROUP BY month
) 

--Add a clean DATE column
--Stores standardized order date
ALTER TABLE public.superstore
ADD COLUMN order_date_clean DATE;


UPDATE public.superstore
SET order_date_clean =
    CASE
        WHEN "ORDER_DATE" LIKE '%/%'
            THEN TO_DATE("ORDER_DATE", 'MM/DD/YYYY')
        ELSE
            TO_DATE("ORDER_DATE", 'YYYY-MM-DD')
    END;


SELECT
    DATE_TRUNC('month', order_date_clean) AS month,
    SUM("sales")
FROM public.superstore
GROUP BY month;


--Top 3 Products per Category
--Uses CTE + DENSE_RANK
WITH product_sales AS (
    SELECT
        "category",
        "PRODUCT_ID",
        SUM("sales") AS total_sales
    FROM public.superstore
    GROUP BY "category", "PRODUCT_ID"
),
ranked_products AS (
    SELECT
        "category",
        "PRODUCT_ID",
        total_sales,
        DENSE_RANK() OVER (
            PARTITION BY "category"
            ORDER BY total_sales DESC
        ) AS rank_val
    FROM product_sales
)
SELECT *
FROM ranked_products
WHERE rank_val <= 3
ORDER BY "category", rank_val;


--Rank Customers by Sales per Region
--Uses DENSE_RANK for tie handling
SELECT
    "REGION",
    "CUSTOMER_NAME",
    total_sales,
    DENSE_RANK() OVER (
        PARTITION BY "REGION"
        ORDER BY total_sales DESC
    ) AS rank_in_region
FROM (
    SELECT
        "REGION",
        "CUSTOMER_NAME",
        SUM("sales") AS total_sales
    FROM public.superstore
    GROUP BY "REGION", "CUSTOMER_NAME"
)t
ORDER BY "REGION", rank_in_region;


--Final Month-over-Month Growth
--Uses clean date column
SELECT
    month,
    monthly_sales,
    prev_month_sales,
    mom_growth
FROM (
    SELECT
        DATE_TRUNC('month', order_date_clean) AS month,
        SUM("sales") AS monthly_sales,
        LAG(SUM("sales")) OVER (
            ORDER BY DATE_TRUNC('month', order_date_clean)
        ) AS prev_month_sales,
        SUM("sales") - LAG(SUM("sales")) OVER (
            ORDER BY DATE_TRUNC('month', order_date_clean)
        ) AS mom_growth
    FROM public.superstore
    GROUP BY month
) t
ORDER BY month;

