import java.io.{File, PrintWriter, BufferedReader, FileReader}
import scalaj.http._
object StockDataFetcher {
  def fetchStockData(symbol: String, apiKey: String): String = {
    val url = "https://www.alphavantage.co/query"
    val response = Http(url)
        .param("function", "TIME_SERIES_DAILY")
        .param("symbol", symbol)
        .param("outputsize", "full") // Fetch full historical data
        .param("apikey", apiKey)
        .param("datatype", "csv") // Get data in CSV format
        .asString

    if (response.isSuccess) response.body else "Error fetching data"
  }

  def saveToFile(content: String, directory: String, filename: String): Unit = {
    val dir = new File(directory)
    if (!dir.exists()) dir.mkdirs()

    val file = new File(dir, filename)
    val pw = new PrintWriter(file)
    try {
      pw.write(content)
    } finally {
      pw.close()
    }
  }

  def readTickersFromFile(filePath: String): List[String] = {
    val br = new BufferedReader(new FileReader(filePath))
    try {
      // Skip the header line if your CSV file has one
      br.readLine()

      Iterator.continually(br.readLine())
          .takeWhile(_ != null)
          .map(_.split(",")(0)) // Split each line by comma and take the first element
          .toList
    } finally {
      br.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val apiKey = "GQYI8P0BU34P6SJH"
    val tickersFilePath = "src/main/resources/constituents.csv"
    val directory = "src/main/fetched_data"

    val tickers = readTickersFromFile(tickersFilePath)

    tickers.foreach { ticker =>
      println(s"Fetching data for $ticker")
      val stockData = fetchStockData(ticker, apiKey)
      saveToFile(stockData, directory, s"${ticker}.csv")
      Thread.sleep(1500) // 15 seconds delay to avoid hitting rate limits
    }
  }
}