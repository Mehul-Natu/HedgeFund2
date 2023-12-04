import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.io.File
// This is to extract the eigenvalues in batches
object BatchFeatureExtraction extends App {
    val spark: SparkSession = SparkSession.builder()
        .appName("StockFeatureExtraction")
        .master("local[*]")
        .getOrCreate()
    import spark.implicits._

    // Function to process a single CSV file and return eigenvalues
    // Eigenvalues include: DailyReturn, SMA, RSI, bollinger bands, VWAP, ROC
    // I just analyzed the stock price data ot the nearest 30 days in our dataset
    def processFile(filePath: String): DataFrame = {
        val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath).limit(30)
        val windowSpec = Window.orderBy("timestamp")

        // calculate: DailyReturn
        val dailyReturn = df.withColumn("dailyReturn", (col("Close") - lag("Close", 1).over(windowSpec)) / lag("Close", 1).over(windowSpec))

        // calculate: SMA
        val movingAverage30 = df
            .withColumn("SMA30", avg("Close").over(windowSpec.rowsBetween(-29, 0)))

        // calculate: RSI
        val gain = dailyReturn.withColumn("Gain", when(col("dailyReturn") > 0, col("dailyReturn")).otherwise(0))
        val loss = dailyReturn.withColumn("Loss", when(col("dailyReturn") < 0, -col("dailyReturn")).otherwise(0))
        val avgGain = gain.withColumn("AvgGain", avg("Gain").over(windowSpec.rowsBetween(-29, -1)))
        val avgLoss = loss.withColumn("AvgLoss", avg("Loss").over(windowSpec.rowsBetween(-29, -1)))
        val rs = avgGain.join(avgLoss, "timestamp").withColumn("RS", col("AvgGain") / col("AvgLoss"))
        val rsi = rs.withColumn("RSI", lit(100) - (lit(100) / (col("RS") + 1)))

        // calculate: bollinger bands
        val stdDev = stddev("Close").over(windowSpec.rowsBetween(-29, 0))
        val bollingerUpper = movingAverage30.withColumn("BollingerUpper", col("SMA30") + (stdDev * lit(2)))
        val bollingerLower = movingAverage30.withColumn("BollingerLower", col("SMA30") - (stdDev * lit(2)))

        // calculate VWAP
        val vwap = df.withColumn("VWAP", sum($"Close" * $"Volume").over(windowSpec) / sum("Volume").over(windowSpec))

        // calculate: ROC
        val closeLag = lag("Close", 1).over(windowSpec)
        val roc = df.withColumn("ROC", ($"Close" - closeLag) / closeLag)

        // join all eigenvalues into one DF
        val resultDf = df
            .join(movingAverage30.select("timestamp", "SMA30"), Seq("timestamp"))
            .join(rsi.select("timestamp", "RSI"), Seq("timestamp"))
            .join(bollingerUpper.select("timestamp", "BollingerUpper"), Seq("timestamp"))
            .join(bollingerLower.select("timestamp", "BollingerLower"), Seq("timestamp"))
            .join(vwap.select("timestamp", "VWAP"), Seq("timestamp"))
            .join(roc.select("timestamp", "ROC"), Seq("timestamp"))
            .orderBy("timestamp")

        // get eigenvalues: calculate the avg of every column, then join into a new DF
        val first30RowsDf = resultDf.limit(30)
        first30RowsDf.select(
            lit(new File(filePath).getName).as("Stock_Name"),
            avg("SMA30").as("avg_SMA30"),
            avg("RSI").as("avg_RSI"),
            avg("BollingerUpper").as("avg_BollingerUpper"),
            avg("BollingerLower").as("avg_BollingerLower"),
            avg("VWAP").as("avg_VWAP"),
            avg("ROC").as("avg_ROC")
        )
    }

    // I divided our data from src/main/fetched_data into distributed_data.
    // Only this way worked. See: src/main/scala/FileDistributor
    // Directory containing CSV files
    val directoryPath = "src/main/distributed_data/folder13"
    val directory = new File(directoryPath)
    val files = directory.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv"))

    // Process each file and collect the results
    val allEigenvalues = files.map(file => processFile(file.getPath)).reduce(_ union _)

    // Write the file with extracted eigenvalues into a new CSV file
    allEigenvalues.coalesce(1).write.option("header", "true").csv("src/main/data_with_feature/temp13")

    println("Batch processing complete.")

    spark.stop()
}
