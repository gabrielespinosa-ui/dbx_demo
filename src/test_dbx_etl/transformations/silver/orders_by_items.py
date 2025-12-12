from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="orders_by_items",
)
def orders_by_items():
    orders_df = spark.readStream.table("orders_bronze")
    items_df = spark.readStream.table("items_bronze")
    return (
        orders_df.join(items_df, on="item_id", how="inner")
        .filter(col("order_status") == "Completed")
        .select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.order_status,
            orders_df.order_date,
            items_df.item_id,
            items_df.product_name.alias("item_name"),
            items_df.price.alias("item_price"),
        )
    )
