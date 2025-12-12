from pyspark import pipelines as dp
from pyspark.sql.functions import count_distinct, sum, col, year, month


@dp.materialized_view(name="orders_by_items_agg")
def orders_by_items_agg():
    orders_by_items_df = spark.read.table("orders_by_items")
    orders_by_items_df = orders_by_items_df.withColumn("year", year(col("order_date"))).withColumn(
        "month", month(col("order_date"))
    )
    return orders_by_items_df.groupBy("item_id", "item_name", "year", "month").agg(
        count_distinct("order_id").alias("total_orders"),
        sum("item_price").alias("total_revenue"),
    )
