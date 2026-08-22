#!/usr/bin/env python3
"""
Validate Parquet file for NaN, Infinity, and NULL values.
This script checks if the dataset is ready for XGBoost training.

Requirements: pip install pandas pyarrow numpy
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path


def validate_parquet(parquet_path):
    """
    Validate a Parquet file for problematic values.

    Args:
        parquet_path: Path to the Parquet file to validate

    Returns:
        0 if validation passes, 1 if issues found
    """
    print("=" * 100)
    print(f"VALIDATING PARQUET FILE: {parquet_path}")
    print("=" * 100)

    # Check if file exists
    if not Path(parquet_path).exists():
        print(f"❌ ERROR: File not found: {parquet_path}")
        return 1

    try:
        # Read Parquet file
        print(f"\n[1/5] Reading Parquet file...")
        df = pd.read_parquet(parquet_path)

        total_rows = len(df)
        total_cols = len(df.columns)

        print(f"  ✓ Loaded {total_rows:,} rows × {total_cols} columns")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Identify numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"\n[2/5] Found {len(numeric_cols)} numeric columns")

        # Check for NULLs
        print(f"\n[3/5] Checking for NULL values...")
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0].sort_values(ascending=False)

        if len(cols_with_nulls) > 0:
            print(f"  ⚠ Found {len(cols_with_nulls)} columns with NULL values:")
            for col_name, null_count in cols_with_nulls.items():
                pct = 100.0 * null_count / total_rows
                print(f"    - {col_name}: {null_count:,} NULLs ({pct:.2f}%)")
        else:
            print(f"  ✓ No NULL values found")

        # Check for NaN in numeric columns
        print(f"\n[4/5] Checking for NaN values in numeric columns...")
        nan_counts = {}
        for col in numeric_cols:
            nan_count = df[col].isna().sum()
            if nan_count > 0:
                nan_counts[col] = nan_count

        if nan_counts:
            print(f"  ❌ CRITICAL: Found {len(nan_counts)} columns with NaN values:")
            for col_name, nan_count in sorted(nan_counts.items(), key=lambda x: -x[1])[:20]:
                pct = 100.0 * nan_count / total_rows
                print(f"    - {col_name}: {nan_count:,} NaNs ({pct:.2f}%)")
                # Show sample values
                sample_nans = df[df[col_name].isna()].head(3)
                if len(sample_nans) > 0:
                    print(f"      Sample row indices: {sample_nans.index.tolist()}")
        else:
            print(f"  ✓ No NaN values found")

        # Check for Infinity in numeric columns
        print(f"\n[5/5] Checking for Infinity values in numeric columns...")
        inf_counts = {}
        for col in numeric_cols:
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                inf_counts[col] = inf_count

        if inf_counts:
            print(f"  ❌ CRITICAL: Found {len(inf_counts)} columns with Infinity values:")
            for col_name, inf_count in sorted(inf_counts.items(), key=lambda x: -x[1])[:20]:
                pct = 100.0 * inf_count / total_rows
                # Count positive vs negative infinity
                pos_inf = (df[col_name] == np.inf).sum()
                neg_inf = (df[col_name] == -np.inf).sum()
                print(f"    - {col_name}: {inf_count:,} Infinity ({pct:.2f}%) [+inf:{pos_inf}, -inf:{neg_inf}]")
                # Show sample values
                sample_infs = df[np.isinf(df[col_name])].head(3)
                if len(sample_infs) > 0:
                    print(f"      Sample row indices: {sample_infs.index.tolist()}")
        else:
            print(f"  ✓ No Infinity values found")

        # Check "features" column if it exists (vector column from Spark ML)
        if "features" in df.columns:
            print(f"\n[BONUS] Checking 'features' vector column...")

            # Try to parse features (it's a Spark ML vector type)
            try:
                # Sample first 100 rows
                sample_size = min(100, total_rows)
                has_vector_issues = False

                for i in range(sample_size):
                    features = df.loc[i, "features"]

                    if features is None or pd.isna(features):
                        print(f"  ❌ Row {i}: features vector is NULL")
                        has_vector_issues = True
                        break

                    # Try to extract array from Spark ML vector
                    # The vector might be stored as different types depending on how it was saved
                    if hasattr(features, 'toArray'):
                        arr = features.toArray()
                    elif isinstance(features, np.ndarray):
                        arr = features
                    elif isinstance(features, (list, tuple)):
                        arr = np.array(features)
                    else:
                        # Skip if we can't parse it
                        continue

                    if np.isnan(arr).any():
                        nan_indices = np.where(np.isnan(arr))[0]
                        print(f"  ❌ Row {i}: features vector contains NaN at indices: {nan_indices.tolist()[:10]}")
                        has_vector_issues = True
                        break

                    if np.isinf(arr).any():
                        inf_indices = np.where(np.isinf(arr))[0]
                        print(f"  ❌ Row {i}: features vector contains Infinity at indices: {inf_indices.tolist()[:10]}")
                        has_vector_issues = True
                        break

                if not has_vector_issues:
                    print(f"  ✓ Sampled {sample_size} rows - no issues in 'features' vector")

            except Exception as e:
                print(f"  ⚠ Could not parse 'features' column (might be Spark ML vector format): {e}")

        # Statistics on numeric columns
        print(f"\n[STATS] Numeric column statistics:")
        print(f"  - Min values: {df[numeric_cols].min().min()}")
        print(f"  - Max values: {df[numeric_cols].max().max()}")
        print(f"  - Columns with constant values: {len([c for c in numeric_cols if df[c].nunique() == 1])}")

        # Final summary
        print("\n" + "=" * 100)
        print("VALIDATION SUMMARY")
        print("=" * 100)

        issues_found = []
        if len(cols_with_nulls) > 0:
            issues_found.append(f"NULL values in {len(cols_with_nulls)} columns")
        if nan_counts:
            issues_found.append(f"NaN values in {len(nan_counts)} columns")
        if inf_counts:
            issues_found.append(f"Infinity values in {len(inf_counts)} columns")

        if issues_found:
            print(f"❌ VALIDATION FAILED")
            print(f"   Issues found: {', '.join(issues_found)}")
            print(f"\n💡 Recommendation:")
            print(f"   - Fix these issues in the data pipeline before XGBoost training")
            print(f"   - Check MissingValuesHandler, aggregations, and calculated features")
            print(f"   - Common causes:")
            print(f"     * Division by zero (e.g., ratios, deltas)")
            print(f"     * Aggregations on all-NULL values (Avg([NULL, NULL]) = NaN)")
            print(f"     * Log/Sqrt of negative values")
            return 1
        else:
            print(f"✅ VALIDATION PASSED")
            print(f"   Dataset is clean and ready for XGBoost training")
            print(f"   - Total rows: {total_rows:,}")
            print(f"   - Total columns: {total_cols}")
            print(f"   - Numeric columns: {len(numeric_cols)}")
            return 0

    except Exception as e:
        print(f"\n❌ ERROR: Failed to validate Parquet file")
        print(f"   {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_parquet.py <path_to_parquet>")
        print("\nExamples:")
        print("  python validate_parquet.py work/output/Experience-md-xgb-local-D2-60-7-7/features/extracted_features.parquet")
        print("  python validate_parquet.py work/output/Experience-md-xgb-local-D2-60-7-7/data/join_exploded_train_prepared.parquet")
        sys.exit(1)

    parquet_path = sys.argv[1]
    exit_code = validate_parquet(parquet_path)
    sys.exit(exit_code)
