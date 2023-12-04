import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

object MakeClusters extends App {
    val spark: SparkSession = SparkSession.builder()
        .appName("StockClustering")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    // Load the data
    val filePath = "src/main/data_with_feature/merged_output/part-00000-167ce484-5078-44bb-9e52-b128a908ebee-c000.csv"
    val data = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Assemble the features into a feature vector
    val assembler = new VectorAssembler()
        .setInputCols(Array("avg_SMA30", "avg_RSI", "avg_BollingerUpper", "avg_BollingerLower", "avg_VWAP", "avg_ROC"))
        .setOutputCol("features")

    val featureData = assembler.transform(data)

    // Apply K-means clustering
    val kmeans = new KMeans().setK(7).setSeed(12345L).setFeaturesCol("features").setPredictionCol("Cluster")
    val model = kmeans.fit(featureData)

    // Make predictions
    val predictions = model.transform(featureData)

    // Select relevant columns to display
    // just for showing the result
//    val result = predictions.select("Stock_Name", "Cluster")

    // Select the required columns
    val finalResult = predictions.select("avg_SMA30", "avg_RSI", "avg_BollingerUpper", "avg_BollingerLower", "avg_VWAP", "avg_ROC", "Cluster")

    // Specify the path where you want to save the CSV file
    val outputPath = "src/main/clustered_data"

    // Save the DataFrame to a CSV file
    finalResult.coalesce(1) // This is optional, it merges the output into a single CSV file
        .write.option("header", "true") // Include column headers
        .csv(outputPath)

    // Stop the SparkSession when done
    spark.stop()

}
