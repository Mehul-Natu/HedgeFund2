import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object StockPricePrediction2 {
    def getResultData(inputPath:String,outputPath:String): Unit ={
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("Test01")
            .getOrCreate()
        // specify the field type
        val schema = new StructType()
            .add("timestamp", StringType)
            .add("open", DoubleType)
            .add("high", DoubleType)
            .add("low", DoubleType)
            .add("close", DoubleType)
            .add("volume", IntegerType)
        val dfGpData: DataFrame = spark.read
            .option("header", "true")
            .schema(schema)
            .csv(inputPath)


        prediction_data(spark, dfGpData, "next_open").createOrReplaceTempView("next_open")
        prediction_data(spark, dfGpData, "next_high").createOrReplaceTempView("next_high")
        prediction_data(spark, dfGpData, "next_low").createOrReplaceTempView("next_low")
        prediction_data(spark, dfGpData, "next_close").createOrReplaceTempView("next_close")
        println("predicted result:")
        val result_data = spark.sql(
            """
              |    SELECT
              |        no.timestamp timestamp,
              |        round(nh.prediction,4) AS open,
              |        round(nh.prediction,4) AS high,
              |        round(nl.prediction,4) AS low,
              |        round(nc.prediction,4) AS close
              |    FROM
              |        next_open no
              |    JOIN next_high nh ON no.timestamp = nh.timestamp
              |    JOIN next_low nl ON no.timestamp = nl.timestamp
              |    JOIN next_close nc ON no.timestamp = nc.timestamp
              |
              |""".stripMargin)

        result_data.show()

        result_data.coalesce(1).write
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv(outputPath)
        println("saved into csv!")
    }
    //train the model
    def prediction_data(spark: SparkSession,dfGpData:DataFrame,LabelCol:String): DataFrame ={
        dfGpData.createOrReplaceTempView("t1")

        val featureCols = Array("open", "high", "low", "close")
        val assembler = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("features")

        val assembledData = assembler.transform(dfGpData).na.drop()

        // create label of the next day data:
        val labeledData = assembledData.withColumn("next_open", lead("open", 1).over(Window.orderBy("timestamp")))
            .withColumn("next_high", lead("high", 1).over(Window.orderBy("timestamp")))
            .withColumn("next_low", lead("low", 1).over(Window.orderBy("timestamp")))
            .withColumn("next_close", lead("close", 1).over(Window.orderBy("timestamp")))

        val data = labeledData.select("timestamp", "features", "next_open", "next_high", "next_low", "next_close").na.drop()
        // split the train dataset and test dataset
        val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

        // create a linear regression model
        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol(LabelCol)

        // define hyper-parametric grids for cross-validation
        val paramGrid = new ParamGridBuilder()
            .addGrid(lr.regParam, Array(0.1, 0.01))
            .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
            .build()

        // model evaluator:
        val evaluator = new RegressionEvaluator()
            .setLabelCol(LabelCol)
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        // create a cross-validator
        val cv = new CrossValidator()
            .setEstimator(lr)
            .setEstimatorParamMaps(paramGrid)
            .setEvaluator(evaluator)
            .setNumFolds(3)

        // Fit cross-validation model on training data
        val cvModel = cv.fit(trainingData)

        // retrieve the best model
        val bestModel = cvModel.bestModel.asInstanceOf[LinearRegressionModel]

        val futureData = spark.sql(
            """
              |select
              |  date_add(timestamp, 1) timestamp,
              |  open,
              |  high,
              |  low,
              |  close,
              |  volume
              |from t1
              |order by timestamp desc limit 1
              |""".stripMargin)

        // Use our best model for predicting the stock price
        val dataResult = bestModel.transform(assembler.transform(futureData))

        // Show results:
        val dataFrame = dataResult.select("timestamp", "prediction")
        //RMSE
        //    val rmse = evaluator.evaluate(predictions)
        //    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

        dataFrame
    }


    def main(args: Array[String]): Unit = {
        // substitute the first path with the stock you want to predict
        getResultData("src/main/distributed_data/folder1/CDNS.csv"
            ,"src/main/predicted_data")
    }

}

