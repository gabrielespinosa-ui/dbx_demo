from pyspark import pipelines as dp
from pyspark.sql.functions import sum as spark_sum, col, year, month, avg, count


@dp.table(name="orders_by_customers")
def orders_by_customers():
    orders_df = spark.readStream.table("orders_by_items")
    customers_df = spark.readStream.table("customers_silver")
    return (
        orders_df.join(customers_df, on="customer_id", how="inner")
        .select(
            orders_df.order_id,
            orders_df.customer_id,
            customers_df.name.alias("customer_name"),
            customers_df.email.alias("customer_email"),
            customers_df.updated_at,
            orders_df.order_status,
            orders_df.order_date,
            orders_df.item_price.alias("order_total_amount"),
        )
        .withColumn("year", year(col("updated_at")))
        .withColumn("month", month(col("updated_at")))
        .withColumn("order_total_amount", col("order_total_amount").cast("double"))
    )


@dp.materialized_view(name="orders_by_customers_agg")
def orders_by_customers_agg():
    return (
        spark.read.table("orders_by_customers")
        .groupBy("customer_id", "customer_name", "customer_email", "year", "month")
        .agg(
            spark_sum("order_total_amount").alias("total_spent"),
            avg("order_total_amount").alias("average_order_value"),
            count("order_id").alias("total_orders"),
        )
    )
