from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType

spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

def sales_data_collector_api(spark, filepath):
    schema = StructType([
        StructField("seller_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("buyer_id", IntegerType(), True),
        StructField("sale_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])

    sales_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .schema(schema) \
        .load(filepath)

    partitioned_table_name = "sales_partition"
    sales_df.write.mode("overwrite") \
        .partitionBy("sale_date") \
        .format("parquet") \
        .saveAsTable(partitioned_table_name)

    return partitioned_table_name

if __name__ == "__main__":

    filepath = "file:///home/takeo/sales.csv"
    table_name = sales_data_collector_api(spark, filepath)
    print(f"Data has been written to Hive table: {table_name}")

def product_data_collector_api(spark, filepath):

    product_df = spark.read.parquet(parquet_file_path)

    table_name = "product"
    product_df.write.mode("overwrite").format("parquet").saveAsTable(table_name)

    return table_name

if __name__ == "__main__":

    filepath = "file:///home/takeo/product.csv"

    table_name = product_data_collector_api(spark, filepath)

    print(f"Data has been written to Hive table: {table_name}")


def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):

    product_df = spark.table(product_hive_table)
    sales_df = spark.table(sales_hive_table)

    product_df.createOrReplaceTempView("product")
    sales_df.createOrReplaceTempView("sales")

    query = """
    SELECT DISTINCT s1.buyer_id
    FROM sales s1
    JOIN product p1 ON s1.product_id = p1.product_id
    LEFT JOIN (
        SELECT DISTINCT s2.buyer_id
        FROM sales s2
        JOIN product p2 ON s2.product_id = p2.product_id
        WHERE p2.product_name = 'iPhone'
    ) iphone_buyers ON s1.buyer_id = iphone_buyers.buyer_id
    WHERE p1.product_name = 'S8' AND iphone_buyers.buyer_id IS NULL
    """

    result_df = spark.sql(query)

    result_df.write.mode("overwrite").format("parquet").saveAsTable(target_hive_table)

    return target_hive_table


if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("DataPreparation") \
        .enableHiveSupport() \
        .getOrCreate()


    product_hive_table = "sales_analysis.product"
    sales_hive_table = "sales_analysis.sales_partitioned"
    target_hive_table = "sales_analysis.s8_non_iphone_buyers"


    table_name = data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table)

    print(table_name)

