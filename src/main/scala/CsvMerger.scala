import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


// This is used to merge the CSV files in src/main/data_with_feature
// Get: all stocks with their corresponding eigenvalues
object CsvMerger extends App {
    val spark: SparkSession = SparkSession.builder()
        .appName("CSV Merger")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    // Method to read a CSV file into DataFrame
    def readCsv(filePath: String): DataFrame = {
        spark.read
            .option("header", "true") // assuming header exists
            .csv(filePath)
    }

    // Initialize an empty DataFrame with the appropriate schema
    val schema = Seq("Stock_Name", "avg_SMA30", "avg_RSI", "avg_BollingerUpper", "avg_BollingerLower", "avg_VWAP", "avg_ROC")
    var mergedDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(schema.map(fieldName => StructField(fieldName, StringType, true))))

    // Read and merge CSV files from each temp directory
    for (i <- 1 to 13) {
        val csvFilePath = s"src/main/data_with_feature/temp$i/part-00000*.csv"
        val tempDataFrame = readCsv(csvFilePath)
        mergedDataFrame = mergedDataFrame.union(tempDataFrame)
    }

    // Write the merged DataFrame to a single CSV file
    mergedDataFrame
        .coalesce(1) // to have a single output file
        .write
        .option("header", "true")
        .csv("src/main/data_with_feature/merged_output")

    spark.stop()
}



