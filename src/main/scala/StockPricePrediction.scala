import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag

object StockPricePrediction {
    def main(args: Array[String]): Unit = {
        // Create a SparkSession
        val spark = SparkSession.builder()
            .appName("StockPricePrediction")
            .master("local[*]")
            .getOrCreate()

        // Load the CSV data into a DataFrame
        // Hope that the csv file path could be changed from frontend
        val data = spark.read.option("header", "true").csv("src/main/distributed_data/folder9/BRO.csv")

        // Convert date column to timestamp
        val dataWithTimestamp = data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd"))

        // Define features and label columns
        val featureCols = Array("open", "high", "low", "close", "volume")
        val labelCols = Array("open", "high", "low", "close")

        // Filter data for the last available date
        val lastAvailableDate = dataWithTimestamp.agg(max("timestamp")).collect()(0)(0).toString
        // This date should also be read in from frontend
        val targetDate = "2023-11-25"

        // Calculate the difference between the last available date and the target date
        val dateDiffExpr = s"datediff('$targetDate', '$lastAvailableDate')"

        // Assuming the data follows the same trends, use the most recent data available
        val targetData = dataWithTimestamp
            .filter(col("timestamp") === lastAvailableDate)
            .withColumn("timestamp", expr(s"date_add(timestamp, $dateDiffExpr)"))

        // Cast the columns to DoubleType
        val dataWithNumericCols = targetData
            .withColumn("open", col("open").cast(DoubleType))
            .withColumn("high", col("high").cast(DoubleType))
            .withColumn("low", col("low").cast(DoubleType))
            .withColumn("close", col("close").cast(DoubleType))
            .withColumn("volume", col("volume").cast(DoubleType))

        // Assemble feature vectors
        val assembler = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("features")

        val assembledData = assembler.transform(dataWithNumericCols)

        // Train a Gradient Boosting Regression model for each target variable
        val models = labelCols.map { labelCol =>
            val gbt = new GBTRegressor()
                .setLabelCol(labelCol)
                .setFeaturesCol("features")
                .setMaxIter(10) // Adjust hyperparameters as needed

            val model = gbt.fit(assembledData)
            (labelCol, model)
        }

        // Make predictions for each target variable
        val predictions = models.map { case (labelCol, model) =>
            val predictions = model.transform(assembledData)
            predictions.select("timestamp", "prediction").withColumnRenamed("prediction", labelCol)
        }

        // Show the predictions in a DataFrame
        val result = predictions.reduce { (df1, df2) =>
            df1.join(df2, Seq("timestamp"), "inner")
        }

        result.show()   // the result is shown in the console, and the result dataframe should be shown to the front-end
        spark.stop()
    }
}
