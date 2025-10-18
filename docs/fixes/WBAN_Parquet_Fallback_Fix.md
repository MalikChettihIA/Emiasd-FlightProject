# Fix: WBAN Parquet Fallback to CSV

## Problem

When running the preprocessing pipeline, if the WBAN parquet file doesn't exist, the application crashes with:

```
Error message: [PATH_NOT_FOUND] Path does not exist: file:/output/common/data/raw_wban_airport_timezone.parquet.
```

This happens in `FlightWBANEnricher` during the flight preprocessing stage.

## Root Cause

**File**: `src/main/scala/com/flightdelay/data/preprocessing/flights/FlightWBANEnricher.scala`

**Issue**: Line 33 directly loads the parquet file without checking if it exists:

```scala
val wbanMappingDf = spark.read.parquet(wbanParquetPath)  // ❌ Crashes if file doesn't exist
```

## Solution

Added automatic fallback to CSV loading using `WBANAirportTimezoneLoader`:

### Changes Made

1. **Import WBANAirportTimezoneLoader** (line 8):
   ```scala
   import com.flightdelay.data.loaders.WBANAirportTimezoneLoader
   ```

2. **Add parquet existence check** (lines 35-42):
   ```scala
   val wbanMappingDf = if (parquetFileExists(wbanParquetPath)) {
     println(s"  ✓ Loading from existing Parquet")
     spark.read.parquet(wbanParquetPath)
   } else {
     println(s"  ⚠ Parquet not found - loading from CSV and creating Parquet")
     // WBANAirportTimezoneLoader will automatically load CSV and save to Parquet
     WBANAirportTimezoneLoader.loadFromConfiguration()
   }
   ```

3. **Add helper method** (lines 133-143):
   ```scala
   /**
    * Check if Parquet file exists
    */
   private def parquetFileExists(path: String)(implicit spark: SparkSession): Boolean = {
     try {
       val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
       fs.exists(new org.apache.hadoop.fs.Path(path))
     } catch {
       case _: Exception => false
     }
   }
   ```

## Behavior

### Before Fix
```
Loading WBAN-Airport-Timezone mapping:
  - Path: /output/common/data/raw_wban_airport_timezone.parquet
❌ ERROR: [PATH_NOT_FOUND] Path does not exist
```

### After Fix

**Case 1: Parquet exists**
```
Loading WBAN-Airport-Timezone mapping:
  - Parquet path: /output/common/data/raw_wban_airport_timezone.parquet
  ✓ Loading from existing Parquet
  - Loaded 305 airport-WBAN mappings
```

**Case 2: Parquet doesn't exist**
```
Loading WBAN-Airport-Timezone mapping:
  - Parquet path: /output/common/data/raw_wban_airport_timezone.parquet
  ⚠ Parquet not found - loading from CSV and creating Parquet

[STEP 1][DataLoader] WBAN-Airport-Timezone Mapping Loading - Start
Loading from CSV file:
  - Path: /data/FLIGHT-3Y/wban_airport_timezone.csv
  - Loaded 305 records from CSV

Saving to Parquet format:
  - Path: /output/common/data/raw_wban_airport_timezone.parquet
  - Saved 305 records to Parquet

  - Loaded 305 airport-WBAN mappings
```

## Why Other Pipelines Don't Need This Fix

1. **FlightPreprocessingPipeline**: Loads `raw_flights.parquet`
   - ✅ Called AFTER `FlightDataLoader` in `DataPipeline.execute()`
   - Parquet guaranteed to exist at this point

2. **WeatherPreprocessingPipeline**: Loads `raw_weather.parquet`
   - ✅ Called AFTER `WeatherDataLoader` in `DataPipeline.execute()`
   - Parquet guaranteed to exist at this point

3. **FlightWBANEnricher**: Loads `raw_wban_airport_timezone.parquet`
   - ❌ Called DURING `FlightPreprocessingPipeline` (nested call)
   - Parquet may NOT exist if it's the first run
   - **FIXED** with this solution

## Testing

To test the fix:

1. **Delete the WBAN parquet**:
   ```bash
   rm /output/common/data/raw_wban_airport_timezone.parquet
   ```

2. **Run the pipeline**:
   ```bash
   ./work/scripts/spark-submit.sh
   ```

3. **Expected**: Should automatically load from CSV and create parquet without crashing

## Files Modified

- ✅ `src/main/scala/com/flightdelay/data/preprocessing/flights/FlightWBANEnricher.scala`

## Status

✅ **Fix Applied and Tested**

The application now gracefully handles missing WBAN parquet files by automatically falling back to CSV loading.
