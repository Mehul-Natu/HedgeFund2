import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StockDataAggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Stock Data Aggregator")
        .master("local[*]") // Use local[*] for testing. Replace with appropriate master in production.
        .getOrCreate()

    import spark.implicits._

    val directoryPath = "src/main/fetched_data" // 替换为您的目录路径
    val allFiles = new java.io.File(directoryPath).listFiles.filter(_.getName.endsWith(".csv"))

    var allStocksDF: DataFrame = spark.emptyDataFrame

    for (file <- allFiles) {
      var df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(file.getAbsolutePath)

      // 格式化日期
      df = df.withColumn("timestamp", to_date($"timestamp", "yyyy-MM-dd"))

      // 处理缺失值：选择删除或填充缺失值
      df = df.na.drop()

      // 为了标识每个股票，添加一个股票代码列（基于文件名）
      val stockSymbol = file.getName.split("\\.")(0) // 假设文件名即股票代码
      df = df.withColumn("StockSymbol", lit(stockSymbol))

      // 合并到总的 DataFrame
      allStocksDF = allStocksDF.union(df)
    }

    // 查看合并后的数据
    allStocksDF.show()

  }
}
