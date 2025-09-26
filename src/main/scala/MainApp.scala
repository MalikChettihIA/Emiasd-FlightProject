import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Iris Data Classification")
      .getOrCreate() // create a Spark session

    spark.sparkContext.setLogLevel("ERROR")

    val dataPath = args(0) // path to the data file
    val data = loadData(spark, dataPath)
    data.show(10, truncate = false)

    val features = extractFeatures(data)
    features.show(10, truncate = false)

    val model = trainModel(features)
    val modelPath = args(1) + "/iris_model" // path to save the model
    println(s"Saving model to $modelPath")
    model.write.overwrite().save(modelPath) // save the model

    spark.stop() // stop the Spark session
  }

  private def loadData(spark: SparkSession, dataPath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataPath)
      .orderBy(rand(seed=42)) // shuffle the data
  }

  private def extractFeatures(data: DataFrame): DataFrame = {
    // convert Species column to numerical values
    val indexedData = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .fit(data)
      .transform(data)

    // merge the features into a single column
    val featureColumns = Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")
    val assembledData = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("raw_features")
      .transform(indexedData)

    // normalize the features columns to [0, 1]
    val scaledData = new MinMaxScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .fit(assembledData)
      .transform(assembledData)
      .select("features", "label")

    scaledData
  }

  private def trainModel(data: DataFrame): RandomForestClassificationModel = {
    // split the data into training and test sets
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 42)
    val trainingDataCached = trainingData.cache()  // cache the training data
    val testDataCached = testData.cache()  // cache the test data

    // train the model
    val model = new RandomForestClassifier()
      .setSeed(42)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("gini")
      .setNumTrees(10)
      .fit(trainingDataCached)

    // make predictions
    val predictions = model.transform(testDataCached)

    // evaluate the model
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("=====================================================")
    println(s"Accuracy: $accuracy")
    println("=====================================================")

    model
  }

}