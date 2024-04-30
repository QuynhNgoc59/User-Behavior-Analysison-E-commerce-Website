--1. Calculate the total visit, pageview, transaction for January, Febuary and March 2017 (order by month)
SELECT
     format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
     SUM(totals.visits) AS visits,
     SUM(totals.pageviews) AS pageviews,
     SUM(totals.transactions) AS transactions,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;

 --2. Calculate the ounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT
      trafficSource.source AS source,
      SUM(totals.visits) AS total_visits,
      SUM(totals.Bounces) AS total_no_of_bounces,
      (SUM(totals.Bounces)/SUM(totals.visits))* 100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;

--3. Calculate the Revenue by traffic source by week & month in June 2017
WITH 
month_data AS(
  SELECT
       "Month" AS time_type,
       format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
       trafficSource.source AS source,
       SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
        unnest(hits) hits,
        unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  ORDER BY revenue DESC
),
week_data as(
  SELECT
       "Week" as time_type,
       FORMAT_DATE("%Y%W", PARSE_DATE("%Y%m%d", date)) as date,
       trafficSource.source AS source,
       SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
        unnest(hits) hits,
        unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  ORDER BY revenue DESC
)

SELECT * FROM month_data
UNION all
SELECT * FROM week_data;

--4. Calculate the Average number of pageviews by purchaser type (purchasers & non-purchasers) in June & July 2017
WITH 
purchaser_data AS(
  SELECT
       format_date("%Y%m",PARSE_DATE("%Y%m%d",DATE)) AS month,
       (SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid)) AS avg_pageviews_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
       ,unnest(hits) hits
       ,unnest(product) product
  WHERE _table_suffix between '0601' and '0731' 
        and totals.transactions>=1
        and product.productRevenue is not null
  GROUP BY month
),

non_purchaser_data AS(
  SELECT
        format_date("%Y%m",PARSE_DATE("%Y%m%d",DATE)) AS month,
        SUM(totals.pageviews)/COUNT(distinct fullvisitorid) AS avg_pageviews_non_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  WHERE _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  GROUP BY month
)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
FULL JOIN non_purchaser_data USING(month)
ORDER BY pd.month;

--5. Calculate the Average number of transactions per user that made a purchase in July 2017
SELECT
    format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    SUM(totals.transactions)/COUNT(DISTINCT fullvisitorid) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
WHERE  totals.transactions>=1
       and totals.totalTransactionRevenue is not null
       and product.productRevenue is not null
GROUP BY month;


--6. Calcualte the Average amount of money spent per session, only include purchaser data in July 2017
SELECT
    format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    ((SUM(product.productRevenue)/SUM(totals.visits))/POWER(10,6)) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
     ,unnest(hits) hits
     ,unnest(product) product
WHERE product.productRevenue is not null
GROUP BY month;


--7. Get other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017
WITH product AS (SELECT DISTINCT fullVisitorId
                       ,product.v2ProductName
                       ,product.productQuantity
                 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
                ,UNNEST (hits) hits
                ,UNNEST (hits.product) product
                 WHERE product.v2ProductName = "YouTube Men's Vintage Henley" AND (totals.transactions >=1 AND product.productRevenue is not null))

    ,total_product AS (SELECT DISTINCT fullVisitorId
                             ,product.v2ProductName
                             ,product.productQuantity
                       FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
                       ,UNNEST (hits) hits
                       ,UNNEST (hits.product) product
                       WHERE fullVisitorId IN (SELECT DISTINCT fullVisitorId FROM product) 
                       AND (totals.transactions >=1 AND product.productRevenue is not null))

SELECT v2ProductName AS other_purchased_products
      ,SUM(productQuantity) AS quantity
FROM total_product
WHERE v2ProductName != "YouTube Men's Vintage Henley" 
GROUP BY v2ProductName;

--8. Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017
Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
WITH action_type AS (SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) AS month
                            ,eCommerceAction.action_type
                            ,product.productRevenue
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
                    ,UNNEST (hits) hits
                    ,UNNEST (hits.product) product
                     WHERE _table_suffix between '0101' and '0331' AND eCommerceAction.action_type IN ('2','3','6'))
     ,num_count AS (SELECT month
                          ,SUM(CASE WHEN ACTION_TYPE.action_type = '2' THEN 1 ELSE 0 END) AS num_product_view
                          ,SUM(CASE WHEN ACTION_TYPE.action_type = '3' THEN 1 ELSE 0 END) AS num_addtocart
                          ,SUM(CASE WHEN ACTION_TYPE.action_type = '6' AND productRevenue is not null THEN 1 ELSE 0 END) AS num_purchase
                    FROM action_type
                    GROUP BY month)
SELECT *
      ,ROUND((num_addtocart/num_product_view)*100,2) AS add_to_cart_rate
      ,ROUND((num_purchase/num_product_view)*100,2) AS purchase_rate
FROM num_count
GROUP BY month, num_product_view, num_addtocart, num_purchase
ORDER BY month 
