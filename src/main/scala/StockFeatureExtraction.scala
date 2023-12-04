import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
// This method is for working out the eigenvalue of a single stock.
object StockFeatureExtraction extends App {
  val spark: SparkSession = SparkSession.builder()
      .appName("StockFeatureExtraction2")
      .master("local[*]")
      .getOrCreate()
  import spark.implicits._

  // read Stock Price Dataframe
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/fetched_data/AAL.csv").limit(30)

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

  // calculate: VWAP
  val vwap = df.withColumn("VWAP", sum($"Close" * $"Volume").over(windowSpec) / sum("Volume").over(windowSpec))

  // calculate: ROC
  val closeLag = lag("Close", 1).over(windowSpec)
  val roc = df.withColumn("ROC", ($"Close" - closeLag) / closeLag)

  // join all eigenvalues into the same DataFrame
  val resultDf = df
      .join(movingAverage30.select("timestamp", "SMA30"), Seq("timestamp"))
      .join(rsi.select("timestamp", "RSI"), Seq("timestamp"))
      .join(bollingerUpper.select("timestamp", "BollingerUpper"), Seq("timestamp"))
      .join(bollingerLower.select("timestamp", "BollingerLower"), Seq("timestamp"))
      .join(vwap.select("timestamp", "VWAP"), Seq("timestamp"))
      .join(roc.select("timestamp", "ROC"), Seq("timestamp"))
      .orderBy("timestamp")

  resultDf.show()

  // Filter the DataFrame to get only the first 30 rows
  val first30RowsDf = resultDf.limit(30)

  // Calculate the average for each column
  val averages = first30RowsDf
      .select(
        avg("SMA30").as("avg_SMA30"),
        avg("RSI").as("avg_RSI"),
        avg("BollingerUpper").as("avg_BollingerUpper"),
        avg("BollingerLower").as("avg_BollingerLower"),
        avg("VWAP").as("avg_VWAP"),
        avg("ROC").as("avg_ROC")
      )

  // Show the result
  averages.show()

  spark.stop()
}
