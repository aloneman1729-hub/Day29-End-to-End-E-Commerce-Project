package com.day29ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

case class CleanOrder(
  orderId: String,
  customerId: String,
  productId: String,
  orderDate: String,
  quantity: Int,
  unitPrice: Double,
  status: String
)

object EcommerceETLApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day29-End-to-End-E-Commerce-Project")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rawPath = "data/raw/orders.csv"
    val customerPath = "data/reference/customers.csv"
    val productPath = "data/reference/products.csv"
    val outputPath = "output"

    println()
    println("============================================================")
    println(" DAY 29 - END-TO-END E-COMMERCE ETL PROJECT")
    println("============================================================")

    // =========================================================
    // 1. RAW INGESTION USING RDD
    // =========================================================

    println()
    println("STEP 1 - RAW INGESTION")
    println("----------------------")

    val rawLines = spark.sparkContext.textFile(rawPath)

    val header = rawLines.first()

    val rawData = rawLines
      .filter(_ != header)

    println(s"Raw records: ${rawData.count()}")

    // =========================================================
    // 2. CLEANING USING RDD
    // =========================================================

    println()
    println("STEP 2 - CLEANING WITH RDD")
    println("--------------------------")

    val parsedOrders = rawData.flatMap { line =>

      val fields = line.split(",", -1)

      if (fields.length == 7) {

        try {

          val orderId = fields(0).trim
          val customerId = fields(1).trim
          val productId = fields(2).trim
          val orderDate = fields(3).trim
          val quantity = fields(4).trim.toInt
          val unitPrice = fields(5).trim.toDouble
          val status = fields(6).trim

          if (
            orderId.nonEmpty &&
            customerId.nonEmpty &&
            productId.nonEmpty &&
            orderDate.nonEmpty &&
            quantity > 0 &&
            unitPrice >= 0 &&
            status.nonEmpty
          ) {
            Some(
              CleanOrder(
                orderId,
                customerId,
                productId,
                orderDate,
                quantity,
                unitPrice,
                status
              )
            )
          } else {
            None
          }

        } catch {
          case _: NumberFormatException => None
        }

      } else {
        None
      }
    }

    val cleanedOrders = parsedOrders
      .map(order => (order.orderId, order))
      .reduceByKey((first, _) => first)
      .map(_._2)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val cleanedCount = cleanedOrders.count()

    println(s"Clean unique records: $cleanedCount")

    val invalidCount = rawData.count() - cleanedCount

    println(s"Removed invalid/duplicate records: $invalidCount")

    // =========================================================
    // 3. PAIR RDD AGGREGATION
    // =========================================================

    println()
    println("STEP 3 - PAIR RDD PRODUCT REVENUE")
    println("---------------------------------")

    val productRevenueRDD = cleanedOrders
      .filter(_.status == "COMPLETED")
      .map(order =>
        (
          order.productId,
          order.quantity * order.unitPrice
        )
      )
      .reduceByKey(_ + _)

    val productRevenue = productRevenueRDD
      .collect()
      .sortBy(_._1)

    productRevenue.foreach {
      case (productId, revenue) =>
        println(
          f"$productId -> revenue = $$${revenue}%.2f"
        )
    }

    // =========================================================
    // 4. CONVERT CLEAN RDD TO DATAFRAME
    // =========================================================

    println()
    println("STEP 4 - CLEAN DATAFRAME")
    println("------------------------")

    val ordersDF = cleanedOrders
      .toDS()
      .withColumn("order_date", to_date($"orderDate"))
      .withColumn(
        "revenue",
        $"quantity" * $"unitPrice"
      )
      .filter($"status" === "COMPLETED")
      .drop("orderDate")

    ordersDF.show(false)

    // =========================================================
    // 5. READ REFERENCE DATA
    // =========================================================

    println()
    println("STEP 5 - REFERENCE DATA")
    println("-----------------------")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customerPath)

    val productsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productPath)

    println("Customers:")
    customersDF.show(false)

    println("Products:")
    productsDF.show(false)

    // =========================================================
    // 6. JOINS - ENRICH ORDERS
    // =========================================================

    println()
    println("STEP 6 - DATA ENRICHMENT WITH JOINS")
    println("-----------------------------------")

    val enrichedDF = ordersDF
      .join(
        broadcast(customersDF),
        ordersDF("customerId") === customersDF("customer_id"),
        "left"
      )
      .join(
        broadcast(productsDF),
        ordersDF("productId") === productsDF("product_id"),
        "left"
      )
      .select(
        ordersDF("orderId"),
        ordersDF("customerId"),
        col("customer_name"),
        col("city"),
        col("segment"),
        ordersDF("productId"),
        col("product_name"),
        col("category"),
        col("brand"),
        ordersDF("order_date"),
        ordersDF("quantity"),
        ordersDF("unitPrice"),
        ordersDF("revenue")
      )

    println("Enriched orders:")
    enrichedDF.show(false)

    // =========================================================
    // 7. PERSIST ENRICHED DATA
    // =========================================================

    println()
    println("STEP 7 - PERSISTENCE")
    println("--------------------")

    val persistedDF = enrichedDF
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Persisted records: ${persistedDF.count()}")

    // =========================================================
    // 8. WINDOW FUNCTIONS
    // =========================================================

    println()
    println("STEP 8 - WINDOW FUNCTIONS")
    println("-------------------------")

    val customerWindow =
      org.apache.spark.sql.expressions.Window
        .partitionBy("customerId")
        .orderBy(
          col("order_date").asc,
          col("orderId").asc
        )

    val cumulativeWindow =
      customerWindow.rowsBetween(
        org.apache.spark.sql.expressions.Window.unboundedPreceding,
        org.apache.spark.sql.expressions.Window.currentRow
      )

    val windowedDF = persistedDF
      .withColumn(
        "customer_order_number",
        row_number().over(customerWindow)
      )
      .withColumn(
        "customer_cumulative_revenue",
        sum("revenue").over(cumulativeWindow)
      )

    windowedDF
      .select(
        "customerId",
        "customer_name",
        "orderId",
        "order_date",
        "revenue",
        "customer_order_number",
        "customer_cumulative_revenue"
      )
      .orderBy("customerId", "order_date", "orderId")
      .show(false)

    // =========================================================
    // 9. CATEGORY AGGREGATION
    // =========================================================

    println()
    println("STEP 9 - CATEGORY AGGREGATION")
    println("-----------------------------")

    val categorySummary = persistedDF
      .groupBy("category")
      .agg(
        countDistinct("orderId").alias("order_count"),
        sum("quantity").alias("units_sold"),
        round(sum("revenue"), 2).alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_order_revenue")
      )
      .orderBy(desc("total_revenue"))

    categorySummary.show(false)

    // =========================================================
    // 10. CUSTOMER REVENUE RANKING
    // =========================================================

    println()
    println("STEP 10 - CUSTOMER REVENUE RANKING")
    println("----------------------------------")

    val customerSummary = persistedDF
      .groupBy(
        "customerId",
        "customer_name",
        "city",
        "segment"
      )
      .agg(
        countDistinct("orderId").alias("orders"),
        sum("quantity").alias("units"),
        round(sum("revenue"), 2).alias("total_revenue")
      )

    val customerRankingWindow =
      org.apache.spark.sql.expressions.Window
        .orderBy(desc("total_revenue"))

    val rankedCustomers = customerSummary
      .withColumn(
        "revenue_rank",
        dense_rank().over(customerRankingWindow)
      )
      .orderBy("revenue_rank")

    rankedCustomers.show(false)

    // =========================================================
    // 11. DAILY SALES AGGREGATION
    // =========================================================

    println()
    println("STEP 11 - DAILY SALES")
    println("---------------------")

    val dailySales = persistedDF
      .groupBy("order_date")
      .agg(
        countDistinct("orderId").alias("orders"),
        sum("quantity").alias("units"),
        round(sum("revenue"), 2).alias("revenue")
      )
      .orderBy("order_date")

    dailySales.show(false)

    // =========================================================
    // 12. WRITE PARTITIONED OUTPUT
    // =========================================================

    println()
    println("STEP 12 - WRITE PARTITIONED OUTPUT")
    println("----------------------------------")

    windowedDF
      .write
      .mode("overwrite")
      .partitionBy("order_date")
      .parquet(s"$outputPath/enriched_orders")

    categorySummary
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/category_summary")

    rankedCustomers
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/customer_ranking")

    dailySales
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/daily_sales")

    println("Output written successfully.")

    // =========================================================
    // 13. PHYSICAL EXECUTION PLAN
    // =========================================================

    println()
    println("STEP 13 - PHYSICAL EXECUTION PLAN")
    println("---------------------------------")

    categorySummary.explain(true)

    // =========================================================
    // 14. CLEANUP
    // =========================================================

    persistedDF.unpersist()
    cleanedOrders.unpersist()

    spark.stop()

    println()
    println("============================================================")
    println(" E-COMMERCE ETL PIPELINE COMPLETED SUCCESSFULLY")
    println("============================================================")
  }
}
