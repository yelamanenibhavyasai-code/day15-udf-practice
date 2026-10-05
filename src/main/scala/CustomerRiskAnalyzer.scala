import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerRiskAnalyzer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 15 - UDF Practice")
      .master("local[*]")
      .getOrCreate()

    // Read transaction data
    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    println("\n==============================")
    println("TRANSACTION DATA")
    println("==============================")

    transactions.show()

    // Scala function for risk classification
    def classifyRisk(amount: Double): String = {
      if (amount < 10000)
        "Low"
      else if (amount <= 50000)
        "Medium"
      else
        "High"
    }

    // Create Scala UDF
    val riskUDF = udf(classifyRisk _)

    // Use UDF with withColumn
    val riskDF = transactions.withColumn(
      "RiskCategory",
      riskUDF(col("TransactionAmount"))
    )

    println("\n==============================")
    println("RISK CATEGORY USING UDF")
    println("==============================")

    riskDF.show()

    // Compare with built-in Spark function
    val builtInDF = transactions.withColumn(
      "RiskCategory",
      when(col("TransactionAmount") < 10000, "Low")
        .when(col("TransactionAmount") <= 50000, "Medium")
        .otherwise("High")
    )

    println("\n==============================")
    println("RISK CATEGORY USING BUILT-IN FUNCTION")
    println("==============================")

    builtInDF.show()

    // Register UDF with Spark SQL catalog
    spark.udf.register(
      "classifyRisk",
      (amount: Double) => classifyRisk(amount)
    )

    riskDF.createOrReplaceTempView("transactions")

    println("\n==============================")
    println("RISK CATEGORY USING SPARK SQL UDF")
    println("==============================")

    spark.sql(
      """
        |SELECT
        |  TransactionID,
        |  CustomerID,
        |  TransactionAmount,
        |  classifyRisk(TransactionAmount) AS RiskCategory
        |FROM transactions
        |""".stripMargin
    ).show()

    // Risk summary
    println("\n==============================")
    println("RISK SUMMARY")
    println("==============================")

    riskDF
      .groupBy("RiskCategory")
      .count()
      .orderBy("RiskCategory")
      .show()

    spark.stop()
  }
}
