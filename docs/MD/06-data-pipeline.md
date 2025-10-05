# 📊 Data Pipeline Guide

Complete guide to data loading, preprocessing, cleaning, and label generation in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Data Loading](#data-loading)
- [Preprocessing Pipeline](#preprocessing-pipeline)
- [Data Cleaning](#data-cleaning)
- [Label Generation](#label-generation)
- [Data Leakage Prevention](#data-leakage-prevention)
- [Feature Generation](#feature-generation)
- [Data Balancing](#data-balancing)
- [Output and Storage](#output-and-storage)
- [Data Quality Validation](#data-quality-validation)

---

## Overview

The data pipeline transforms **raw flight and weather CSV files** into **ML-ready preprocessed data** with features and labels.

### Pipeline Objectives

1. **Load** flight, weather, and airport data from CSV/TXT files
2. **Clean** data by handling nulls, outliers, and invalid values
3. **Generate** delay classification labels (15, 30, 45, 60 minutes)
4. **Prevent** data leakage by removing post-flight information
5. **Engineer** time-based and route-based features
6. **Balance** classes to handle imbalanced datasets
7. **Validate** schema and data quality
8. **Save** preprocessed data for reuse across experiments

### Key Characteristics

- **Shared Preprocessing**: Run once, reused by all experiments
- **Spark DataFrames**: Distributed processing for scalability
- **Immutable Transformations**: Functional pipeline stages
- **Parquet Storage**: Efficient columnar format with compression

---

## Pipeline Architecture

### High-Level Flow

```
┌────────────────────────────────────────────────────────────────┐
│ STEP 1: DATA LOADING                                           │
├────────────────────────────────────────────────────────────────┤
│ FlightDataLoader.loadFromConfiguration()                       │
│   • Load Flights CSV (201201.csv)                              │
│   • Load Weather TXT (201201hourly.txt)                        │
│   • Load Airport Mapping (wban_airport_timezone.csv)           │
│   • Join on WBAN codes                                         │
│   • Save to raw_flights.parquet                                │
│                                                                 │
│ Output: ~142K flights with 65 columns (21 flight + 44 weather) │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 2: PREPROCESSING PIPELINE                                 │
├────────────────────────────────────────────────────────────────┤
│ FlightPreprocessingPipeline.execute()                          │
│   ↓                                                             │
│ 1. FlightDataCleaner                                           │
│    • Drop nulls in critical fields                             │
│    • Handle outliers                                           │
│    • Type conversions                                          │
│   ↓                                                             │
│ 2. FlightDataGenerator                                         │
│    • Generate time-based features                              │
│    • Generate route features                                   │
│    • Generate distance categories                              │
│   ↓                                                             │
│ 3. FlightLabelGenerator                                        │
│    • Create delay labels (15, 30, 45, 60 min)                  │
│    • Fill missing delay values                                 │
│    • Create weather/NAS delay indicators                       │
│   ↓                                                             │
│ 4. FlightDataLeakageCleaner                                    │
│    • Remove ARR_DELAY_NEW, WEATHER_DELAY, NAS_DELAY            │
│    • Keep only pre-flight features                             │
│   ↓                                                             │
│ 5. FlightDataBalancer                                          │
│    • Balance classes (SMOTE, undersampling)                    │
│    • Optional: only when needed                                │
│   ↓                                                             │
│ 6. Schema Validation                                           │
│    • Validate required columns                                 │
│    • Validate types                                            │
│    • Verify leakage removal                                    │
│   ↓                                                             │
│ Save to: processed_flights.parquet                             │
│                                                                 │
│ Output: ~140K flights with ~50 features + 4 labels             │
└────────────────────────────────────────────────────────────────┘
```

### Pipeline Stages

| Stage | Class | Purpose | Input Cols | Output Cols |
|-------|-------|---------|------------|-------------|
| **1. Clean** | `FlightDataCleaner` | Handle nulls, outliers | 65 | ~60 (cleaned) |
| **2. Generate** | `FlightDataGenerator` | Engineer features | ~60 | ~65 (+ 5 features) |
| **3. Label** | `FlightLabelGenerator` | Create delay labels | ~65 | ~75 (+ 10 labels) |
| **4. Leakage** | `FlightDataLeakageCleaner` | Remove post-flight data | ~75 | ~70 (- 5 leakage cols) |
| **5. Balance** | `FlightDataBalancer` | Balance classes | ~70 | ~70 (resampled) |

---

## Data Loading

### FlightDataLoader

**Class**: `com.flightdelay.data.loaders.FlightDataLoader`

**Purpose**: Load and join flight, weather, and airport data

### Data Sources

```yaml
common:
  data:
    flight:
      path: "/data/FLIGHT-3Y/Flights/201201.csv"
    weather:
      path: "/data/FLIGHT-3Y/Weather/201201hourly.txt"
    airportMapping:
      path: "/data/FLIGHT-3Y/wban_airport_timezone.csv"
```

### Loading Process

```scala
def loadFromConfiguration()(implicit config: AppConfiguration,
                                     spark: SparkSession): DataFrame = {
  // 1. Load flight CSV
  val flightData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(config.common.data.flight.path)

  // 2. Load weather TXT
  val weatherData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(config.common.data.weather.path)

  // 3. Load airport mapping
  val airportMapping = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(config.common.data.airportMapping.path)

  // 4. Join datasets
  val joined = flightData
    .join(airportMapping, flightData("ORIGIN") === airportMapping("airport_code"))
    .join(weatherData, ...)

  // 5. Save to parquet
  joined.write.mode("overwrite").parquet("raw_flights.parquet")

  joined
}
```

### Flight Data Schema (21 columns)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `FL_DATE` | Date | Flight date | 2012-01-01 |
| `OP_CARRIER` | String | Carrier code | AA, UA, DL |
| `OP_CARRIER_AIRLINE_ID` | Int | Airline ID | 19805 |
| `OP_CARRIER_FL_NUM` | Int | Flight number | 1234 |
| `ORIGIN_AIRPORT_ID` | Int | Origin airport ID | 12478 |
| `DEST_AIRPORT_ID` | Int | Destination airport ID | 14771 |
| `CRS_DEP_TIME` | Int | Scheduled departure time | 1530 (3:30 PM) |
| `CRS_ARR_TIME` | Int | Scheduled arrival time | 1830 (6:30 PM) |
| `CRS_ELAPSED_TIME` | Double | Scheduled flight time (min) | 180.0 |
| `DISTANCE` | Double | Distance (miles) | 1500.0 |
| `ARR_DELAY_NEW` | Double | Arrival delay (minutes) | 15.0 |
| `WEATHER_DELAY` | Double | Weather delay component | 5.0 |
| `NAS_DELAY` | Double | NAS delay component | 10.0 |
| ... | ... | ... | ... |

### Weather Data Schema (44 columns)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `WBAN` | Int | Weather station ID | 12345 |
| `Date` | Date | Observation date | 2012-01-01 |
| `Time` | Int | Observation time (HHMM) | 1500 |
| `DryBulbFarenheit` | Double | Temperature (°F) | 45.0 |
| `Visibility` | Double | Visibility (miles) | 10.0 |
| `WindSpeed` | Double | Wind speed (mph) | 15.0 |
| `PrecipitationOneHour` | Double | Hourly precipitation (inches) | 0.1 |
| ... | ... | ... | ... |

### Output

**File**: `/output/common/data/raw_flights.parquet`

**Format**: Parquet (columnar, compressed)

**Size**: ~142,000 flights, 65 columns

---

## Preprocessing Pipeline

### FlightPreprocessingPipeline

**Class**: `com.flightdelay.data.preprocessing.FlightPreprocessingPipeline`

**Entry Point**:

```scala
FlightPreprocessingPipeline.execute()(
  implicit spark: SparkSession,
  configuration: AppConfiguration
): DataFrame
```

### Pipeline Execution

```scala
def execute(): DataFrame = {
  // Load raw data
  val rawData = spark.read.parquet("raw_flights.parquet")

  // Stage 1: Clean
  val cleaned = FlightDataCleaner.preprocess(rawData)

  // Stage 2: Generate features
  val generated = FlightDataGenerator.preprocess(cleaned)

  // Stage 3: Generate labels
  val labeled = FlightLabelGenerator.preprocess(generated)

  // Stage 4: Remove leakage
  val noLeakage = FlightDataLeakageCleaner.preprocess(labeled)

  // Stage 5: Balance classes
  val balanced = FlightDataBalancer.preprocess(noLeakage)

  // Validate schema
  validatePreprocessedSchema(balanced)

  // Save preprocessed data
  balanced.write.mode("overwrite").parquet("processed_flights.parquet")

  balanced
}
```

---

## Data Cleaning

### FlightDataCleaner

**Class**: `com.flightdelay.data.preprocessing.FlightDataCleaner`

**Purpose**: Clean data by handling nulls, outliers, and invalid values

### Cleaning Operations

#### 1. Drop Null Values in Critical Fields

```scala
// Drop rows with nulls in critical fields
val cleaned = df.na.drop(Seq(
  "FL_DATE",
  "OP_CARRIER_AIRLINE_ID",
  "OP_CARRIER_FL_NUM",
  "ORIGIN_AIRPORT_ID",
  "DEST_AIRPORT_ID",
  "CRS_DEP_TIME",
  "CRS_ELAPSED_TIME"
))
```

**Dropped**: ~2,000-3,000 rows with missing critical data

#### 2. Handle Outliers

```scala
// Cap extreme delays (> 500 minutes = 8+ hours)
val cappedDelays = df.withColumn(
  "ARR_DELAY_NEW",
  when(col("ARR_DELAY_NEW") > 500, 500).otherwise(col("ARR_DELAY_NEW"))
)
```

#### 3. Type Conversions

```scala
// Ensure correct types
val typed = df
  .withColumn("FL_DATE", to_date(col("FL_DATE")))
  .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast(DoubleType))
```

### Output

**Result**: Cleaned DataFrame with ~140K flights (98% retention)

**Removed**: ~2K flights with critical nulls or invalid data

---

## Label Generation

### FlightLabelGenerator

**Class**: `com.flightdelay.data.preprocessing.FlightLabelGenerator`

**Purpose**: Generate binary classification labels for different delay thresholds

### Label Types

#### 1. Basic Delay Labels (Primary)

```scala
// Binary labels for different delay thresholds
"label_is_delayed_15min" -> when(col("ARR_DELAY_NEW") >= 15.0, 1).otherwise(0),
"label_is_delayed_30min" -> when(col("ARR_DELAY_NEW") >= 30.0, 1).otherwise(0),
"label_is_delayed_45min" -> when(col("ARR_DELAY_NEW") >= 45.0, 1).otherwise(0),
"label_is_delayed_60min" -> when(col("ARR_DELAY_NEW") >= 60.0, 1).otherwise(0),
```

| Label | Condition | Description |
|-------|-----------|-------------|
| `label_is_delayed_15min` | `ARR_DELAY_NEW >= 15` | Delayed ≥ 15 minutes |
| `label_is_delayed_30min` | `ARR_DELAY_NEW >= 30` | Delayed ≥ 30 minutes |
| `label_is_delayed_45min` | `ARR_DELAY_NEW >= 45` | Delayed ≥ 45 minutes |
| `label_is_delayed_60min` | `ARR_DELAY_NEW >= 60` | Delayed ≥ 60 minutes |

**Usage**: Select one as target label in experiment configuration

#### 2. Filled Delay Values

```scala
// Fill nulls with 0 (interpret as "no delay of this type")
"label_arr_delay_filled" ->
  when(col("ARR_DELAY_NEW").isNull, 0.0).otherwise(col("ARR_DELAY_NEW")),

"label_weather_delay_filled" ->
  when(col("WEATHER_DELAY").isNull, 0.0).otherwise(col("WEATHER_DELAY")),

"label_nas_delay_filled" ->
  when(col("NAS_DELAY").isNull, 0.0).otherwise(col("NAS_DELAY")),
```

**Rationale**: Null delay interpreted as "no delay of this type"

#### 3. Delay Type Indicators

```scala
// Weather delay indicator
"label_has_weather_delay" ->
  when(col("label_weather_delay_filled") > 0, 1).otherwise(0),

// NAS delay indicator
"label_has_nas_delay" ->
  when(col("label_nas_delay_filled") > 0, 1).otherwise(0),

// Any weather or NAS delay
"label_has_any_weather_nas_delay" ->
  when(col("label_weather_delay_filled") > 0 ||
       col("label_nas_delay_filled") > 0, 1).otherwise(0),
```

#### 4. On-Time Indicators

```scala
// On-time flight (no delay)
"label_is_on_time" -> when(col("label_arr_delay_filled") <= 0, 1).otherwise(0),

// Early flight (negative delay)
"label_is_early" -> when(col("label_arr_delay_filled") < 0, 1).otherwise(0),
```

### Label Distribution Example

For `label_is_delayed_15min` on ~140K flights:

```
Class 0 (not delayed): ~100,000 flights (71%)
Class 1 (delayed ≥15min): ~40,000 flights (29%)

Imbalance ratio: 2.5:1
```

### Delay Thresholds Explained

| Threshold | Business Meaning | Use Case |
|-----------|------------------|----------|
| **15 min** | FAA delay definition | General delay prediction |
| **30 min** | Significant delay | Connection risk |
| **45 min** | Major delay | Passenger compensation threshold (some airlines) |
| **60 min** | Severe delay | Flight disruption |

---

## Data Leakage Prevention

### FlightDataLeakageCleaner

**Class**: `com.flightdelay.data.preprocessing.FlightDataLeakageCleaner`

**Purpose**: Remove columns that contain **post-flight information** not available at prediction time

### Data Leakage Problem

**Definition**: Using information in training that won't be available at prediction time

**Example**:
```
Training: Use ARR_DELAY_NEW to predict label_is_delayed_15min
Problem: ARR_DELAY_NEW is only known AFTER flight lands
Result: Overfitting, unrealistic performance
```

### Removed Columns (Data Leakage)

```scala
val leakageColumns = Seq(
  "ARR_DELAY_NEW",      // Actual arrival delay (target variable source)
  "WEATHER_DELAY",      // Weather delay component (only known after)
  "NAS_DELAY",          // NAS delay component (only known after)
  "ARR_TIME",           // Actual arrival time (only known after)
  "DEP_TIME",           // Actual departure time (only known after)
  "DEP_DELAY",          // Departure delay (correlated with arrival delay)
  "CARRIER_DELAY",      // Delay components only known after flight
  "SECURITY_DELAY",
  "LATE_AIRCRAFT_DELAY"
)

val noLeakage = df.drop(leakageColumns: _*)
```

### Kept Columns (Pre-Flight Features)

**Available before flight**:

- `FL_DATE` - Flight date
- `CRS_DEP_TIME` - Scheduled departure time
- `CRS_ARR_TIME` - Scheduled arrival time
- `CRS_ELAPSED_TIME` - Scheduled flight duration
- `DISTANCE` - Route distance
- `ORIGIN_AIRPORT_ID`, `DEST_AIRPORT_ID` - Route
- `OP_CARRIER` - Airline
- Weather observations at scheduled departure time

**Generated features** (see next section):

- `feature_departure_hour`
- `feature_flight_month`
- `feature_is_weekend`
- `feature_route_id`
- ...

### Validation

The pipeline validates leakage removal:

```scala
val leakageColumns = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
leakageColumns.foreach { colName =>
  if (df.columns.contains(colName)) {
    throw new RuntimeException(s"LEAKAGE DETECTED: $colName should have been removed!")
  }
}
```

Output:
```
Validating leakage columns removal:
  - ARR_DELAY_NEW removed (no leakage)
  - WEATHER_DELAY removed (no leakage)
  - NAS_DELAY removed (no leakage)
```

---

## Feature Generation

### FlightDataGenerator

**Class**: `com.flightdelay.data.preprocessing.FlightDataGenerator`

**Purpose**: Engineer derived features from raw data

### Generated Features

#### 1. Time-Based Features

```scala
// Extract hour from scheduled departure time
"feature_departure_hour" ->
  (col("CRS_DEP_TIME") / 100).cast(IntegerType),

// Extract month
"feature_flight_month" ->
  month(col("FL_DATE")),

// Extract year
"feature_flight_year" ->
  year(col("FL_DATE")),

// Quarter (1-4)
"feature_flight_quarter" ->
  quarter(col("FL_DATE")),

// Day of week (1=Monday, 7=Sunday)
"feature_flight_day_of_week" ->
  dayofweek(col("FL_DATE")),

// Is weekend (1=yes, 0=no)
"feature_is_weekend" ->
  when(dayofweek(col("FL_DATE")).isin(1, 7), 1).otherwise(0),
```

**Examples**:
```
CRS_DEP_TIME: 1530 → feature_departure_hour: 15
FL_DATE: 2012-01-15 → feature_flight_month: 1
FL_DATE: 2012-01-15 (Sunday) → feature_is_weekend: 1
```

#### 2. Route Features

```scala
// Unique route identifier
"feature_route_id" ->
  concat(col("ORIGIN_AIRPORT_ID"), lit("-"), col("DEST_AIRPORT_ID")),

// Route popularity (frequency count)
"feature_route_frequency" ->
  count("*").over(Window.partitionBy("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID")),
```

**Example**:
```
ORIGIN_AIRPORT_ID: 12478, DEST_AIRPORT_ID: 14771
→ feature_route_id: "12478-14771"
```

#### 3. Distance Categories

```scala
// Categorize flights by distance
"feature_distance_category" ->
  when(col("DISTANCE") < 500, "short")      // < 500 miles
    .when(col("DISTANCE") < 1500, "medium") // 500-1500 miles
    .otherwise("long"),                     // > 1500 miles
```

**Categories**:
- Short: < 500 miles (e.g., NYC → Boston)
- Medium: 500-1500 miles (e.g., NYC → Chicago)
- Long: > 1500 miles (e.g., NYC → LA)

### Feature Engineering Principles

1. **Only pre-flight information** - Must be available before takeoff
2. **Domain knowledge** - Based on aviation expertise
3. **Temporal features** - Capture seasonality and time patterns
4. **Route features** - Capture route-specific behavior

---

## Data Balancing

### FlightDataBalancer

**Class**: `com.flightdelay.data.preprocessing.FlightDataBalancer`

**Purpose**: Balance class distribution to handle imbalanced datasets

### Imbalance Problem

**Example**:
```
Class 0 (not delayed): 100,000 flights (71%)
Class 1 (delayed ≥15min): 40,000 flights (29%)

Imbalance ratio: 2.5:1
```

**Impact**: Model biased toward majority class (predicts "not delayed" too often)

### Balancing Strategies

#### Strategy 1: Undersampling

**Technique**: Reduce majority class to match minority class

```scala
val positive = df.filter(col("label") === 1)
val negative = df.filter(col("label") === 0)

val positiveCount = positive.count()
val negativeCount = negative.count()

// Sample majority class
val sampledNegative = negative.sample(
  withReplacement = false,
  fraction = positiveCount.toDouble / negativeCount
)

val balanced = positive.union(sampledNegative)
```

**Result**: 50/50 class distribution, but **loses data**

#### Strategy 2: SMOTE (Synthetic Minority Over-sampling Technique)

**Technique**: Generate synthetic samples for minority class

**Note**: Currently placeholder in `FlightDataBalancer`

### When to Balance

**Use balancing**:
- ✅ Severe imbalance (> 5:1 ratio)
- ✅ Binary classification
- ✅ Model overfits to majority class

**Don't balance**:
- ❌ Mild imbalance (< 3:1 ratio)
- ❌ Use class weights instead (in Random Forest)
- ❌ Evaluation metric already accounts for imbalance (F1, AUC)

**Current Approach**: No balancing by default; Random Forest handles mild imbalance well

---

## Output and Storage

### Preprocessed Data Storage

**Location**: `/output/common/data/processed_flights.parquet`

**Format**: Parquet (Snappy compression)

**Schema**: ~50 features + 10 labels

**Size**: ~140,000 flights

**Reuse**: Loaded by all experiments (shared preprocessing)

### Parquet Benefits

1. **Columnar Storage**: Only read columns you need
2. **Compression**: Snappy compression (~5x smaller than CSV)
3. **Schema Preservation**: Types and metadata stored
4. **Partitioning**: Can partition by date, route, etc.
5. **Compatibility**: Works with Spark, Pandas, Arrow, etc.

### Loading Preprocessed Data

**In experiments**:

```scala
val processedParquetPath = s"${config.common.output.basePath}/common/data/processed_flights.parquet"
val processedData = spark.read.parquet(processedParquetPath)
```

**Benefits**:
- Skip preprocessing on each experiment
- Faster iteration
- Consistent data across experiments

---

## Data Quality Validation

### Schema Validation

The pipeline validates schema after preprocessing:

```scala
def validatePreprocessedSchema(df: DataFrame): Unit = {
  // 1. Validate base columns with types
  requiredBaseColumns.foreach { case (colName, expectedType) =>
    assert(df.columns.contains(colName), s"Missing column: $colName")
    assert(df.schema(colName).dataType == expectedType,
           s"Wrong type for $colName")
  }

  // 2. Validate generated columns
  requiredGeneratedColumns.foreach { colName =>
    assert(df.columns.contains(colName), s"Missing column: $colName")
  }

  // 3. Validate label columns
  requiredLabelColumns.foreach { colName =>
    assert(df.columns.contains(colName), s"Missing column: $colName")
  }

  // 4. Validate leakage removal
  leakageColumns.foreach { colName =>
    assert(!df.columns.contains(colName),
           s"LEAKAGE DETECTED: $colName should have been removed!")
  }
}
```

### Validation Output

```
================================================================================
Schema Validation
================================================================================

Validating base columns:
  - FL_DATE (DateType)
  - OP_CARRIER_AIRLINE_ID (IntegerType)
  - OP_CARRIER_FL_NUM (IntegerType)
  - ORIGIN_AIRPORT_ID (IntegerType)
  - DEST_AIRPORT_ID (IntegerType)
  - CRS_DEP_TIME (IntegerType)
  - CRS_ELAPSED_TIME (DoubleType)

Validating generated columns:
  - feature_departure_hour
  - feature_flight_month
  - feature_flight_year
  - feature_flight_quarter
  - feature_flight_day_of_week
  - feature_is_weekend
  - feature_route_id
  - feature_distance_category

Validating label columns:
  - label_arr_delay_filled
  - label_weather_delay_filled
  - label_nas_delay_filled
  - label_is_delayed_15min
  - label_is_delayed_30min
  - label_is_delayed_60min

Validating leakage columns removal:
  - ARR_DELAY_NEW removed (no leakage)
  - WEATHER_DELAY removed (no leakage)
  - NAS_DELAY removed (no leakage)

================================================================================
✓ Schema Validation PASSED - 68 columns validated
================================================================================
```

---

## Pipeline Summary

### Input

```
Raw CSV/TXT files:
- Flights: 201201.csv (~142K rows, 21 columns)
- Weather: 201201hourly.txt (44 columns)
- Airport Mapping: wban_airport_timezone.csv
```

### Output

```
Preprocessed Parquet:
- processed_flights.parquet (~140K rows, ~68 columns)
  - ~50 features (base + generated)
  - ~10 labels (delay thresholds, indicators)
  - No data leakage
  - Schema validated
```

### Transformations

| Stage | Operation | Rows Lost | Columns Added | Columns Removed |
|-------|-----------|-----------|---------------|-----------------|
| **Load** | Join flight + weather + airport | 0 | +44 (weather) | 0 |
| **Clean** | Drop nulls, handle outliers | ~2K (1.4%) | 0 | 0 |
| **Generate** | Create time/route features | 0 | +8 features | 0 |
| **Label** | Create delay labels | 0 | +10 labels | 0 |
| **Leakage** | Remove post-flight columns | 0 | 0 | -5 (leakage) |
| **Balance** | (Optional) Balance classes | Variable | 0 | 0 |

---

## Best Practices

### 1. Always Check for Data Leakage

**Validate** that removed columns are truly removed:

```scala
assert(!df.columns.contains("ARR_DELAY_NEW"), "Leakage detected!")
```

### 2. Handle Missing Values Explicitly

**Don't rely on defaults**:

```scala
// Explicit: Fill with 0
when(col("DELAY").isNull, 0.0).otherwise(col("DELAY"))

// Avoid: Implicit null handling
```

### 3. Validate Schema After Each Stage

**Check columns exist**:

```scala
assert(df.columns.contains("feature_departure_hour"))
```

### 4. Use Parquet for Intermediate Storage

**Save processed data**:

```scala
df.write.mode("overwrite").parquet("processed_data.parquet")
```

### 5. Document Feature Engineering

**Comment derived features**:

```scala
// Extract hour from HHMM format (e.g., 1530 → 15)
"feature_departure_hour" -> (col("CRS_DEP_TIME") / 100).cast(IntegerType)
```

---

## Next Steps

- **[Feature Engineering](07-feature-engineering.md)** - Learn about PCA and feature extraction
- **[ML Pipeline](08-ml-pipeline.md)** - Understand model training
- **[Code Reference](11-code-reference.md)** - Class-by-class documentation

---

**Data Pipeline Guide Complete! Ready to process flight data.** 📊
