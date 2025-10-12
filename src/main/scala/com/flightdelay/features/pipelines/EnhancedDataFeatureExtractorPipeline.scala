package com.flightdelay.features.pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, dayofweek, month, dayofmonth, hour, minute, unix_timestamp, year}
import org.apache.spark.sql.types.{BooleanType, DoubleType, DateType, TimestampType}

/**
 * Enhanced feature pipeline with support for:
 * - Text/Categorical columns (StringIndexer)
 * - Numeric columns
 * - Boolean columns (auto-converted to 0.0/1.0)
 * - Date/Timestamp columns (auto-extracted to temporal features)
 * - Feature scaling
 * - Feature selection
 *
 * @param textCols Categorical columns to encode (String type)
 * @param numericCols Numeric columns (Integer, Long, Float, Double)
 * @param booleanCols Boolean columns (will be converted to 0.0/1.0)
 * @param dateCols Date/Timestamp columns (will be converted to temporal features)
 * @param target Target column name
 * @param maxCat Max categories for VectorIndexer
 * @param handleInvalid Strategy for invalid values
 * @param scalerType Optional scaler type
 * @param featureSelector Optional feature selection
 * @param customStages Additional custom stages
 */
class EnhancedDataFeatureExtractorPipeline(
                                            textCols: Array[String],
                                            numericCols: Array[String],
                                            booleanCols: Array[String] = Array.empty,
                                            dateCols: Array[String] = Array.empty,  // ← NOUVEAU paramètre
                                            target: String,
                                            maxCat: Int = 32,
                                            handleInvalid: String = "skip",
                                            scalerType: Option[String] = None,
                                            featureSelector: Option[(String, Int)] = None,
                                            customStages: Array[PipelineStage] = Array.empty
                                          ) {

  // Column naming constants
  private val _label = "label"
  private val _prefix = "indexed_"
  private val _boolPrefix = "bool_numeric_"
  private val _datePrefix = "date_"
  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "featuresVecIndexed"
  private val _selectedFeatures = "selectedFeatures"
  private val _features = "features"

  // ===========================================================================================
  // PREPROCESSING: Convert boolean and date columns
  // ===========================================================================================

  /**
   * Converts boolean columns to numeric (0.0/1.0)
   */
  private def preprocessBooleans(df: DataFrame): DataFrame = {
    if (booleanCols.isEmpty) {
      df
    } else {
      var result = df
      booleanCols.foreach { colName =>
        result = result.withColumn(
          _boolPrefix + colName,
          when(col(colName) === true, 1.0)
            .when(col(colName) === false, 0.0)
            .otherwise(null)
            .cast(DoubleType)
        )
      }
      result
    }
  }

  /**
   * Extracts temporal features from date/timestamp columns
   * Creates multiple numeric features per date column:
   * - year, month, day, dayofweek, hour, minute (if timestamp)
   * - unix_timestamp (for relative time calculations)
   */
  private def preprocessDates(df: DataFrame): DataFrame = {
    if (dateCols.isEmpty) {
      df
    } else {
      var result = df
      dateCols.foreach { colName =>
        val colType = df.schema(colName).dataType

        // Features communes pour Date et Timestamp
        result = result
          .withColumn(s"${_datePrefix}${colName}_year", year(col(colName)).cast(DoubleType))
          .withColumn(s"${_datePrefix}${colName}_month", month(col(colName)).cast(DoubleType))
          .withColumn(s"${_datePrefix}${colName}_day", dayofmonth(col(colName)).cast(DoubleType))
          .withColumn(s"${_datePrefix}${colName}_dayofweek", dayofweek(col(colName)).cast(DoubleType))
          .withColumn(s"${_datePrefix}${colName}_unix", unix_timestamp(col(colName)).cast(DoubleType))

        // Features additionnelles pour Timestamp
        if (colType == TimestampType) {
          result = result
            .withColumn(s"${_datePrefix}${colName}_hour", hour(col(colName)).cast(DoubleType))
            .withColumn(s"${_datePrefix}${colName}_minute", minute(col(colName)).cast(DoubleType))
        }
      }
      result
    }
  }

  /**
   * Get the list of boolean columns after conversion
   */
  private def getBooleanNumericCols: Array[String] = {
    booleanCols.map(_boolPrefix + _)
  }

  /**
   * Get the list of date-derived numeric columns
   */
  private def getDateNumericCols(df: DataFrame): Array[String] = {
    if (dateCols.isEmpty) {
      Array.empty
    } else {
      dateCols.flatMap { colName =>
        val colType = df.schema(colName).dataType
        val baseFeatures = Seq(
          s"${_datePrefix}${colName}_year",
          s"${_datePrefix}${colName}_month",
          s"${_datePrefix}${colName}_day",
          s"${_datePrefix}${colName}_dayofweek",
          s"${_datePrefix}${colName}_unix"
        )

        if (colType == TimestampType) {
          baseFeatures ++ Seq(
            s"${_datePrefix}${colName}_hour",
            s"${_datePrefix}${colName}_minute"
          )
        } else {
          baseFeatures
        }
      }
    }
  }

  // ===========================================================================================
  // PIPELINE STAGES DEFINITION
  // ===========================================================================================

  /**
   * Stage 1: StringIndexer
   */
  private val stringIndexer = new StringIndexer()
    .setInputCols(textCols ++ Array(target))
    .setOutputCols(textCols.map(_prefix + _) ++ Array(_prefix + target))
    .setHandleInvalid(handleInvalid)

  /**
   * Stage 2: VectorAssembler
   * Note: getDateNumericCols needs preprocessed DataFrame, so this is built dynamically
   */
  private def buildVectorAssembler(preprocessedDF: DataFrame): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(
        textCols.map(_prefix + _) ++
          numericCols ++
          getBooleanNumericCols ++
          getDateNumericCols(preprocessedDF)
      )
      .setOutputCol(_featuresVec)
      .setHandleInvalid(handleInvalid)
  }

  /**
   * Stage 3: VectorIndexer
   */
  private val vectorIndexer = new VectorIndexer()
    .setInputCol(_featuresVec)
    .setOutputCol(_featuresVecIndex)
    .setMaxCategories(maxCat)
    .setHandleInvalid(handleInvalid)

  /**
   * Stage 4: Optional Feature Selector
   */
  private val selectorStage: Option[PipelineStage] = featureSelector.map {
    case ("chi2", numFeatures) =>
      new ChiSqSelector()
        .setNumTopFeatures(numFeatures)
        .setFeaturesCol(_featuresVecIndex)
        .setLabelCol(_prefix + target)
        .setOutputCol(_selectedFeatures)

    case ("anova", numFeatures) =>
      new UnivariateFeatureSelector()
        .setFeatureType("continuous")
        .setLabelType("categorical")
        .setSelectionMode("numTopFeatures")
        .setSelectionThreshold(numFeatures)
        .setFeaturesCol(_featuresVecIndex)
        .setLabelCol(_prefix + target)
        .setOutputCol(_selectedFeatures)

    case (selectorType, _) =>
      throw new IllegalArgumentException(s"Unknown selector type: $selectorType. Use 'chi2' or 'anova'")
  }

  /**
   * Stage 5: Optional Scaler
   */
  private val scalerStage: Option[PipelineStage] = scalerType.map {
    case "standard" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new StandardScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setWithMean(true)
        .setWithStd(true)

    case "minmax" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new MinMaxScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setMin(0.0)
        .setMax(1.0)

    case "robust" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new RobustScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setWithCentering(true)
        .setWithScaling(true)

    case scaler =>
      throw new IllegalArgumentException(s"Unknown scaler type: $scaler. Use 'standard', 'minmax', or 'robust'")
  }

  // ===========================================================================================
  // PIPELINE CONSTRUCTION
  // ===========================================================================================

  private def buildPipeline(preprocessedDF: DataFrame): Pipeline = {
    val vectorAssembler = buildVectorAssembler(preprocessedDF)
    val baseStages = Array(stringIndexer, vectorAssembler, vectorIndexer)
    val withSelector = selectorStage.map(s => baseStages :+ s).getOrElse(baseStages)
    val withScaler = scalerStage.map(s => withSelector :+ s).getOrElse(withSelector)
    val allStages = withScaler ++ customStages

    new Pipeline().setStages(allStages)
  }

  // ===========================================================================================
  // PUBLIC API
  // ===========================================================================================

  /**
   * Fit the pipeline on training data
   * Automatically preprocesses boolean and date columns before fitting
   */
  def fit(data: DataFrame): PipelineModel = {
    val preprocessed = preprocessDates(preprocessBooleans(data))
    buildPipeline(preprocessed).fit(preprocessed)
  }

  /**
   * Fit and transform training data
   */
  def fitTransform(data: DataFrame): (PipelineModel, DataFrame) = {
    val preprocessed = preprocessDates(preprocessBooleans(data))
    val model = buildPipeline(preprocessed).fit(preprocessed)
    val transformed = postProcess(model.transform(preprocessed))
    (model, transformed)
  }

  /**
   * Transform new data using fitted model
   * Automatically preprocesses boolean and date columns before transforming
   */
  def transform(model: PipelineModel, data: DataFrame): DataFrame = {
    val preprocessed = preprocessDates(preprocessBooleans(data))
    postProcess(model.transform(preprocessed))
  }

  private def postProcess(df: DataFrame): DataFrame = {
    val finalFeaturesCol = if (scalerStage.isDefined) {
      _features
    } else if (selectorStage.isDefined) {
      _selectedFeatures
    } else {
      _featuresVecIndex
    }

    df.select(
      col(finalFeaturesCol).as("features"),
      col(_prefix + target).as(_label)
    )
  }

  def getStages: Array[PipelineStage] = {
    // Return dummy pipeline stages for inspection
    // Note: Actual pipeline is built dynamically with preprocessed data
    Array(stringIndexer, vectorIndexer) ++
      selectorStage.toSeq ++
      scalerStage.toSeq ++
      customStages
  }

  def printSummary(): Unit = {
    println("=" * 80)
    println("Enhanced Data Feature Extractor Pipeline Configuration")
    println("=" * 80)
    println(s"Text columns: ${textCols.mkString(", ")}")
    println(s"Numeric columns: ${numericCols.mkString(", ")}")
    println(s"Boolean columns: ${booleanCols.mkString(", ")}")
    println(s"Date columns: ${dateCols.mkString(", ")}")  // ← NOUVEAU
    println(s"Target: $target")
    println(s"Max categories: $maxCat")
    println(s"Handle invalid: $handleInvalid")
    println(s"Scaler: ${scalerType.getOrElse("None")}")
    println(s"Feature selector: ${featureSelector.map(f => s"${f._1} (top ${f._2})").getOrElse("None")}")
    println(s"Custom stages: ${customStages.length}")
    println("=" * 80)

    if (dateCols.nonEmpty) {
      println("\nDate Feature Extraction:")
      println("  Each date column will generate the following features:")
      println("    - year, month, day, dayofweek, unix_timestamp")
      println("  Timestamp columns will additionally generate:")
      println("    - hour, minute")
      println("=" * 80)
    }
  }
}

/**
 * Companion object with builder pattern
 */
object EnhancedDataFeatureExtractorPipeline {

  class Builder {
    private var textCols: Array[String] = Array.empty
    private var numericCols: Array[String] = Array.empty
    private var booleanCols: Array[String] = Array.empty
    private var dateCols: Array[String] = Array.empty  // ← NOUVEAU
    private var target: String = ""
    private var maxCat: Int = 32
    private var handleInvalid: String = "skip"
    private var scalerType: Option[String] = None
    private var featureSelector: Option[(String, Int)] = None
    private var customStages: Array[PipelineStage] = Array.empty

    def withTextCols(cols: Array[String]): Builder = {
      this.textCols = cols
      this
    }

    def withNumericCols(cols: Array[String]): Builder = {
      this.numericCols = cols
      this
    }

    def withBooleanCols(cols: Array[String]): Builder = {
      this.booleanCols = cols
      this
    }

    def withDateCols(cols: Array[String]): Builder = {  // ← NOUVEAU
      this.dateCols = cols
      this
    }

    def withTarget(col: String): Builder = {
      this.target = col
      this
    }

    def withMaxCategories(max: Int): Builder = {
      this.maxCat = max
      this
    }

    def withHandleInvalid(strategy: String): Builder = {
      require(
        strategy == "skip" || strategy == "error" || strategy == "keep",
        "handleInvalid must be 'skip', 'error', or 'keep'"
      )
      this.handleInvalid = strategy
      this
    }

    def withScaler(scaler: String): Builder = {
      this.scalerType = Some(scaler)
      this
    }

    def withoutScaler(): Builder = {
      this.scalerType = None
      this
    }

    def withFeatureSelection(selectorType: String, numFeatures: Int): Builder = {
      this.featureSelector = Some((selectorType, numFeatures))
      this
    }

    def withoutFeatureSelection(): Builder = {
      this.featureSelector = None
      this
    }

    def withCustomStage(stage: PipelineStage): Builder = {
      this.customStages = this.customStages :+ stage
      this
    }

    def withCustomStages(stages: Array[PipelineStage]): Builder = {
      this.customStages = this.customStages ++ stages
      this
    }

    def build(): EnhancedDataFeatureExtractorPipeline = {
      require(
        textCols.nonEmpty || numericCols.nonEmpty || booleanCols.nonEmpty || dateCols.nonEmpty,
        "Must provide at least one feature column (text, numeric, boolean, or date)"
      )
      require(target.nonEmpty, "Target column must be specified")

      new EnhancedDataFeatureExtractorPipeline(
        textCols,
        numericCols,
        booleanCols,
        dateCols,  // ← NOUVEAU
        target,
        maxCat,
        handleInvalid,
        scalerType,
        featureSelector,
        customStages
      )
    }
  }

  def builder(): Builder = new Builder()
}