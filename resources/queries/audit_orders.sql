WITH ItemSales AS (
  SELECT 
    item_id, 
    sum(total_orders) as total_orders
  FROM dbx_test.gabriel_espinosa.orders_by_items_agg 
  WHERE year = '2025' and month = '11' 
  GROUP BY item_id
),
RankedSales AS (
  SELECT 
    *,
    RANK() OVER (ORDER BY total_orders DESC) as sales_rank
  FROM ItemSales
)
SELECT item_id, total_orders
FROM RankedSales
WHERE sales_rank = 1;